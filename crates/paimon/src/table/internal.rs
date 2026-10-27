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

//! Unstable execution bridge for the repository's language bindings.
//!
//! Source plans are the public input to search readers. Candidate and scoring
//! requests below are internal execution stages: Core owns their ordering and
//! global result coordination. Writer adapters forward their existing commit
//! identifiers through the same bridge. This module is available only with `internal-api`.

pub use super::search_task::{SearchTask, SearchTaskExecutor, SearchTaskResult};
use super::{
    BatchVectorRead, CommitMessage, GlobalIndexResult, HybridSearchBuilder, Table, TableWrite,
    VectorRead, VectorScanPlan,
};

pub async fn execute_search_task(
    table: &Table,
    task: &[u8],
    context: &[u8],
) -> crate::Result<Vec<u8>> {
    table.execute_search_task(task, context).await
}

pub async fn read_vector_with_executor(
    reader: &VectorRead,
    plan: VectorScanPlan,
    executor: &dyn SearchTaskExecutor,
) -> crate::Result<GlobalIndexResult> {
    reader.read_with_executor(plan, executor).await
}

pub async fn read_batch_vector_with_executor(
    reader: &BatchVectorRead,
    plan: VectorScanPlan,
    executor: &dyn SearchTaskExecutor,
) -> crate::Result<Vec<GlobalIndexResult>> {
    reader.read_with_executor(plan, executor).await
}

pub async fn read_full_text_with_executor(
    reader: &super::FullTextRead,
    plan: super::FullTextScanPlan,
    executor: &dyn SearchTaskExecutor,
) -> crate::Result<GlobalIndexResult> {
    reader.read_with_executor(plan, executor).await
}

pub async fn execute_hybrid_with_executor(
    builder: &HybridSearchBuilder<'_>,
    executor: &dyn SearchTaskExecutor,
) -> crate::Result<GlobalIndexResult> {
    builder.execute_with_executor(executor).await
}

/// Forward an identifier already supplied by the batch or stream facade.
pub async fn prepare_commit_with_identifier(
    writer: &mut TableWrite,
    commit_identifier: i64,
) -> crate::Result<Vec<CommitMessage>> {
    writer
        .prepare_commit_with_identifier(commit_identifier)
        .await
}
