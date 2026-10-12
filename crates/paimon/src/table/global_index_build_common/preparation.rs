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

//! Bounded preparation of independently owned global-index shards.

use crate::table::{CommitMessage, Table, TableCommit};
use crate::{Error, Result};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::future::Future;
use std::panic::AssertUnwindSafe;

/// Prepare shards against a snapshot selected by the caller. Each shard has its
/// own reader and writer, like Java GenericIndexTopoBuilder.BuildIndexOperator.
///
/// Results are returned in plan order, even if shards finish out of order. On
/// failure, stop admitting work and drain every started shard before cleaning
/// this preparation's private outputs. Dropping in-flight futures is unsafe:
/// their native blocking workers could still be writing the index files.
///
/// This helper owns every returned message until it returns successfully. Never
/// use it to clean up messages already handed to callers or submitted for commit.
pub(crate) async fn prepare_shards<S, F, Fut, T>(
    table: &Table,
    shards: Vec<S>,
    parallelism: usize,
    mut build: F,
) -> Result<Vec<(CommitMessage, T)>>
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Result<Option<(CommitMessage, T)>>>,
{
    if parallelism == 0 {
        return Err(Error::ConfigInvalid {
            message: "Option 'global-index.build.parallelism' must be greater than 0.".into(),
        });
    }
    let mut pending = shards.into_iter().enumerate();
    let mut active = FuturesUnordered::new();
    for _ in 0..parallelism {
        let Some((ordinal, shard)) = pending.next() else {
            break;
        };
        active.push(run_shard(ordinal, build(shard)));
    }
    let mut completed = Vec::new();
    let mut failure = None;
    while let Some((ordinal, result)) = active.next().await {
        match result {
            Ok(Some(output)) => completed.push((ordinal, output)),
            Ok(None) => {}
            Err(error) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        if failure.is_none() {
            if let Some((ordinal, shard)) = pending.next() {
                active.push(run_shard(ordinal, build(shard)));
            }
        }
    }
    if let Some(error) = failure {
        // Every started worker has finished; no file can appear after cleanup.
        let messages = completed
            .into_iter()
            .map(|(_, (message, _))| message)
            .collect::<Vec<_>>();
        abort_private_outputs(table, &messages).await;
        return Err(error);
    }
    completed.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    Ok(completed.into_iter().map(|(_, output)| output).collect())
}

async fn run_shard<Fut, T>(
    ordinal: usize,
    build: Fut,
) -> (usize, Result<Option<(CommitMessage, T)>>)
where
    Fut: Future<Output = Result<Option<(CommitMessage, T)>>>,
{
    let result = AssertUnwindSafe(build)
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| {
            let reason = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            Err(Error::UnexpectedError {
                message: format!("Global index shard {ordinal} panicked: {reason}"),
                source: None,
            })
        });
    (ordinal, result)
}

async fn abort_private_outputs(table: &Table, messages: &[CommitMessage]) {
    if !messages.is_empty() {
        let commit = TableCommit::new(
            table.clone(),
            format!("global-index-private-build-{}", uuid::Uuid::new_v4()),
        );
        // Preserve the build error if best-effort cleanup itself fails.
        let _ = commit.abort(messages).await;
    }
}

#[cfg(test)]
mod tests;
