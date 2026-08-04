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

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use futures::stream::{self, StreamExt};
use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce() + Send + 'static>;

const DEFAULT_IO_BOUND_WORKERS: usize = 32;
const IO_BOUND_WORKERS_PER_CPU: usize = 4;
const WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

struct ExecutorState {
    receiver: Receiver<Job>,
    max_workers: AtomicUsize,
    minimum_workers: usize,
    physical_worker_limit: usize,
    worker_idle_timeout: Duration,
    worker_count: AtomicUsize,
    outstanding_jobs: AtomicUsize,
    next_worker_id: AtomicUsize,
}

struct GlobalIndexExecutor {
    sender: Sender<Job>,
    state: Arc<ExecutorState>,
}

#[derive(Default)]
struct TaskCompletionState {
    started: bool,
    cancelled: bool,
}

struct TaskCompletion {
    state: Mutex<TaskCompletionState>,
}

impl TaskCompletion {
    fn new() -> Self {
        Self {
            state: Mutex::new(TaskCompletionState::default()),
        }
    }

    fn try_start(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.cancelled {
            return false;
        }
        state.started = true;
        true
    }

    fn cancel_if_queued(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.started {
            state.cancelled = true;
        }
    }
}

struct TaskCompletionGuard(Arc<TaskCompletion>);

impl Drop for TaskCompletionGuard {
    fn drop(&mut self) {
        self.0.cancel_if_queued();
    }
}

impl GlobalIndexExecutor {
    fn new(max_workers: usize) -> Self {
        Self::new_with_limits(
            max_workers,
            max_physical_worker_count(),
            WORKER_IDLE_TIMEOUT,
        )
    }

    fn new_with_limits(
        max_workers: usize,
        physical_worker_limit: usize,
        worker_idle_timeout: Duration,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let physical_worker_limit = physical_worker_limit.max(1);
        let max_workers = max_workers.max(1).min(physical_worker_limit);
        Self {
            sender,
            state: Arc::new(ExecutorState {
                receiver,
                max_workers: AtomicUsize::new(max_workers),
                minimum_workers: max_workers,
                physical_worker_limit,
                worker_idle_timeout,
                worker_count: AtomicUsize::new(0),
                outstanding_jobs: AtomicUsize::new(0),
                next_worker_id: AtomicUsize::new(0),
            }),
        }
    }

    fn ensure_capacity(&self, max_workers: usize) {
        let max_workers = max_workers.max(1).min(self.state.physical_worker_limit);
        self.state
            .max_workers
            .fetch_max(max_workers, Ordering::SeqCst);
        // Another query may already have queued work, so raising the high-watermark
        // can immediately make more workers useful even before this caller submits.
        let _ = self.ensure_workers_for_load();
    }

    fn submit(&self, job: Job) -> crate::Result<()> {
        self.state.outstanding_jobs.fetch_add(1, Ordering::SeqCst);
        if let Err(error) = self.ensure_workers_for_load() {
            self.state.outstanding_jobs.fetch_sub(1, Ordering::SeqCst);
            return Err(error);
        }
        if self.sender.send(job).is_err() {
            self.state.outstanding_jobs.fetch_sub(1, Ordering::SeqCst);
            return Err(crate::Error::UnexpectedError {
                message: "global-index executor stopped before accepting a task".to_string(),
                source: None,
            });
        }
        Ok(())
    }

    fn ensure_workers_for_load(&self) -> crate::Result<()> {
        ensure_workers_for_load(&self.state)
    }
}

fn ensure_workers_for_load(state: &Arc<ExecutorState>) -> crate::Result<()> {
    loop {
        let current = state.worker_count.load(Ordering::SeqCst);
        let max_workers = state.max_workers.load(Ordering::SeqCst);
        let outstanding = state.outstanding_jobs.load(Ordering::SeqCst);
        let target = outstanding.min(max_workers);
        if current >= target {
            return Ok(());
        }
        if state
            .worker_count
            .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            continue;
        }

        let worker_id = state.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let worker_state = Arc::clone(state);
        if let Err(error) = std::thread::Builder::new()
            .name(format!("paimon-global-index-{worker_id}"))
            .spawn(move || run_worker(worker_state))
        {
            state.worker_count.fetch_sub(1, Ordering::SeqCst);
            return Err(crate::Error::UnexpectedError {
                message: format!("failed to start global-index executor thread: {error}"),
                source: Some(Box::new(error)),
            });
        }
    }
}

fn run_worker(state: Arc<ExecutorState>) {
    loop {
        match state.receiver.recv_timeout(state.worker_idle_timeout) {
            Ok(job) => {
                let _ = catch_unwind(AssertUnwindSafe(job));
                state.outstanding_jobs.fetch_sub(1, Ordering::SeqCst);
            }
            Err(RecvTimeoutError::Timeout) => {
                if retire_idle_worker(&state) {
                    // A submit racing with retirement may have observed the old worker
                    // count. Re-check after decrementing so queued work can replace us.
                    let _ = ensure_workers_for_load(&state);
                    return;
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                state.worker_count.fetch_sub(1, Ordering::SeqCst);
                return;
            }
        }
    }
}

fn retire_idle_worker(state: &ExecutorState) -> bool {
    loop {
        let current = state.worker_count.load(Ordering::SeqCst);
        if current <= state.minimum_workers {
            return false;
        }
        if state
            .worker_count
            .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return true;
        }
    }
}

fn default_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
}

fn max_physical_worker_count() -> usize {
    default_worker_count()
        .saturating_mul(IO_BOUND_WORKERS_PER_CPU)
        .max(DEFAULT_IO_BOUND_WORKERS)
}

/// Shared global-index executor for synchronous search and range-I/O waits. It
/// starts at the machine's available parallelism and grows lazily, but keeps the
/// configured logical fan-out separate from a physical cap of four workers per
/// CPU (and at least the default 32 I/O workers). Growth workers expire after the
/// same one-minute idle interval used by Java's `GlobalIndexReadThreadPool`.
/// Smaller query limits are enforced by each query's bounded job scheduler.
fn global_executor() -> &'static GlobalIndexExecutor {
    static EXECUTOR: OnceLock<GlobalIndexExecutor> = OnceLock::new();
    EXECUTOR.get_or_init(|| GlobalIndexExecutor::new(default_worker_count()))
}

pub(crate) fn ensure_global_index_executor_capacity(max_workers: usize) {
    global_executor().ensure_capacity(max_workers);
}

/// Runs bounded global-index jobs to completion, restores submission order, and
/// returns the first error by submission index. Dedicated executor jobs cannot be
/// interrupted after they start, so short-circuiting would only hide background
/// work and make the surfaced error depend on completion timing.
pub(crate) async fn drain_indexed_jobs<T, F>(
    jobs: impl Iterator<Item = F>,
    concurrency: usize,
) -> crate::Result<Vec<T>>
where
    F: std::future::Future<Output = crate::Result<T>>,
{
    let indexed = jobs
        .enumerate()
        .map(|(index, job)| async move { (index, job.await) });
    let mut collected: Vec<(usize, crate::Result<T>)> = stream::iter(indexed)
        .buffer_unordered(concurrency.max(1))
        .collect()
        .await;
    collected.sort_by_key(|(index, _)| *index);
    collected.into_iter().map(|(_, result)| result).collect()
}

pub(crate) async fn execute_global_index<T, F>(
    panic_context: &'static str,
    task: F,
) -> crate::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> crate::Result<T> + Send + 'static,
{
    execute_on(global_executor(), panic_context, task).await
}

async fn execute_on<T, F>(
    executor: &GlobalIndexExecutor,
    panic_context: &'static str,
    task: F,
) -> crate::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> crate::Result<T> + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    let completion = Arc::new(TaskCompletion::new());
    let task_completion = Arc::clone(&completion);
    executor.submit(Box::new(move || {
        if !task_completion.try_start() {
            return;
        }
        let outcome = catch_unwind(AssertUnwindSafe(task));
        let _ = sender.send(outcome);
    }))?;
    // Cancellation marks a queued job so the worker skips it. Started work is
    // detached like Tokio's spawn_blocking tasks; Drop must not block a runtime
    // thread because the search may still depend on async range I/O from it. The
    // caller runtime must remain driven until that I/O completes; dropping the
    // runtime cancels the spawned read and releases the worker.
    let _completion_guard = TaskCompletionGuard(completion);

    let outcome = receiver
        .await
        .map_err(|error| crate::Error::UnexpectedError {
            message: "global-index executor dropped a task result".to_string(),
            source: Some(Box::new(error)),
        })?;
    match outcome {
        Ok(result) => result,
        Err(payload) => Err(crate::Error::UnexpectedError {
            message: format!("{panic_context}: {}", panic_message(payload.as_ref())),
            source: None,
        }),
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "non-string panic payload"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Duration;

    #[tokio::test]
    async fn executor_respects_worker_limit() {
        let executor = GlobalIndexExecutor::new(2);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let tasks = (0..8).map(|_| {
            let in_flight = Arc::clone(&in_flight);
            let peak = Arc::clone(&peak);
            execute_on(&executor, "test global-index task failed", move || {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        });

        futures::future::try_join_all(tasks).await.unwrap();
        assert!(peak.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test]
    async fn executor_grows_to_requested_capacity() {
        let executor = Arc::new(GlobalIndexExecutor::new(1));
        executor.ensure_capacity(3);
        let (started_sender, mut started_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (release_sender, release_receiver) = crossbeam_channel::bounded(3);
        let executor_for_tasks = Arc::clone(&executor);
        let tasks = (0..3).map(move |_| {
            let executor = Arc::clone(&executor_for_tasks);
            let started_sender = started_sender.clone();
            let release_receiver = release_receiver.clone();
            async move {
                execute_on(&executor, "test global-index task failed", move || {
                    started_sender.send(()).unwrap();
                    release_receiver.recv().unwrap();
                    Ok(())
                })
                .await
            }
        });
        let join = tokio::spawn(async move { futures::future::try_join_all(tasks).await });

        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(1), started_receiver.recv())
                .await
                .expect("executor did not grow to requested capacity")
                .expect("executor stopped before all tasks started");
        }
        for _ in 0..3 {
            release_sender.send(()).unwrap();
        }
        join.await.unwrap().unwrap();
        assert_eq!(executor.state.worker_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn executor_caps_requested_physical_capacity() {
        let executor = GlobalIndexExecutor::new_with_limits(1, 3, Duration::from_secs(60));
        executor.ensure_capacity(usize::MAX);

        assert_eq!(executor.state.max_workers.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn executor_reclaims_idle_growth() {
        let executor = Arc::new(GlobalIndexExecutor::new_with_limits(
            1,
            3,
            Duration::from_millis(20),
        ));
        executor.ensure_capacity(3);
        let (started_sender, mut started_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (release_sender, release_receiver) = crossbeam_channel::bounded(3);
        let executor_for_tasks = Arc::clone(&executor);
        let tasks = (0..3).map(move |_| {
            let executor = Arc::clone(&executor_for_tasks);
            let started_sender = started_sender.clone();
            let release_receiver = release_receiver.clone();
            async move {
                execute_on(&executor, "test global-index task failed", move || {
                    started_sender.send(()).unwrap();
                    release_receiver.recv().unwrap();
                    Ok(())
                })
                .await
            }
        });
        let join = tokio::spawn(async move { futures::future::try_join_all(tasks).await });

        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(1), started_receiver.recv())
                .await
                .expect("executor did not grow before idle reclamation")
                .expect("executor stopped before all tasks started");
        }
        for _ in 0..3 {
            release_sender.send(()).unwrap();
        }
        join.await.unwrap().unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.state.worker_count.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("executor did not reclaim idle growth workers");

        execute_on(&executor, "test global-index task failed", || Ok(()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn drain_indexed_jobs_bounds_concurrency_and_restores_order() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let jobs = (0..8).map(|index| {
            let in_flight = Arc::clone(&in_flight);
            let peak = Arc::clone(&peak);
            async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis((8 - index) as u64)).await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(index)
            }
        });

        let results = drain_indexed_jobs(jobs, 2).await.unwrap();
        assert_eq!(results, (0..8).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_waiter_does_not_stop_submitted_task() {
        let (started_sender, started_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let (completed_sender, completed_receiver) = oneshot::channel();
        let join = tokio::spawn(async move {
            execute_global_index("test global-index task failed", move || {
                let _ = started_sender.send(());
                release_receiver.recv().unwrap();
                let _ = completed_sender.send(());
                Ok(())
            })
            .await
        });
        started_receiver.await.unwrap();

        join.abort();
        assert!(join.await.unwrap_err().is_cancelled());
        release_sender.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), completed_receiver)
            .await
            .expect("submitted task did not finish after waiter cancellation")
            .expect("submitted task stopped before reporting completion");
    }

    #[test]
    fn cancelling_started_search_keeps_current_thread_runtime_live() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let handle = tokio::runtime::Handle::current();
            let (started_sender, started_receiver) = oneshot::channel();
            let completed = Arc::new(AtomicBool::new(false));
            let completed_for_task = Arc::clone(&completed);
            let join = tokio::spawn(async move {
                execute_global_index("test global-index task failed", move || {
                    let _ = started_sender.send(());
                    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
                    handle.spawn(async move {
                        tokio::task::yield_now().await;
                        let _ = sender.send(());
                    });
                    receiver.recv().unwrap();
                    completed_for_task.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
            });
            started_receiver.await.unwrap();

            join.abort();
            let error = tokio::time::timeout(Duration::from_secs(1), join)
                .await
                .expect("cancelling a search blocked the current-thread runtime")
                .unwrap_err();
            assert!(error.is_cancelled());
            tokio::time::timeout(Duration::from_secs(1), async {
                while !completed.load(Ordering::SeqCst) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("detached search did not finish after caller cancellation");
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_queued_task_returns_without_waiting_for_queue() {
        let executor = Arc::new(GlobalIndexExecutor::new(1));
        let (started_sender, started_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let first_executor = Arc::clone(&executor);
        let first = tokio::spawn(async move {
            execute_on(&first_executor, "first task failed", move || {
                let _ = started_sender.send(());
                release_receiver.recv().unwrap();
                Ok(())
            })
            .await
        });
        started_receiver.await.unwrap();

        let second_ran = Arc::new(AtomicBool::new(false));
        let second_ran_in_task = Arc::clone(&second_ran);
        let second_executor = Arc::clone(&executor);
        let mut second = tokio::spawn(async move {
            execute_on(&second_executor, "second task failed", move || {
                second_ran_in_task.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.state.outstanding_jobs.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second task was not queued");

        second.abort();
        let cancelled_without_waiting = tokio::time::timeout(Duration::from_secs(1), &mut second)
            .await
            .is_ok();

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        if !cancelled_without_waiting {
            assert!(second.await.unwrap_err().is_cancelled());
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while executor.state.outstanding_jobs.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled task was not drained from the executor queue");

        assert!(
            cancelled_without_waiting,
            "cancelling a queued task waited for the task ahead of it"
        );
        assert!(!second_ran.load(Ordering::SeqCst));
    }
}
