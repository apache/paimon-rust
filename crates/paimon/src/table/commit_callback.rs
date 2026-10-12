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

//! Post-publication notifications for Paimon table commits.

use crate::spec::{ManifestEntry, Snapshot};
use crate::Result;
use async_trait::async_trait;

/// The committed snapshot and its data-file delta, after row tracking assignment.
///
/// Entries include both ADD and DELETE records, but not changelog files. This is
/// the snapshot / deltaFiles portion of Java CommitCallback.Context.
#[derive(Debug, Clone)]
pub struct CommitCallbackContext {
    pub snapshot: Snapshot,
    pub commit_entries: Vec<ManifestEntry>,
    pub identifier: i64,
}

/// Invoked in registration order after snapshot publication is confirmed.
///
/// Implementations must be idempotent: a lost response can cause notification
/// to be replayed from the committed snapshot during in-process recovery.
/// Filtering previously committed identifiers does not invoke this notification.
/// Errors are propagated without undoing publication or deleting files. The caller owns callback resources;
/// the committer holds shared references, also across clones.
#[async_trait]
pub trait CommitCallback: Send + Sync {
    async fn call(&self, context: &CommitCallbackContext) -> Result<()>;
}
