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

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn global_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Runtime::new()
            .expect("failed to build global tokio runtime for paimon datafusion integration")
    })
}

/// Returns a [`Handle`] to the global Tokio runtime.
///
/// If a Tokio runtime is already entered on the current thread, its handle is
/// returned directly. Otherwise a lazily-initialised global runtime is used.
pub fn runtime() -> Handle {
    match Handle::try_current() {
        Ok(h) => h,
        _ => global_runtime().handle().clone(),
    }
}

// These helpers work around DataFusion FFI callbacks that may run without an
// entered Tokio runtime. See https://github.com/apache/datafusion/issues/16312.
// A global OnceLock<Runtime> avoids creating a new runtime on every call;
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
    if Handle::try_current().is_ok() {
        let handle = global_runtime().handle().clone();
        std::thread::spawn(move || handle.block_on(future))
            .join()
            .expect(panic_error)
    } else {
        global_runtime().block_on(future)
    }
}
