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

//! SnapshotCommit abstraction for atomic snapshot commits.
//!
//! Reference: [pypaimon snapshot_commit.py](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/snapshot/snapshot_commit.py)

use crate::api::rest_api::RESTApi;
use crate::catalog::Identifier;
use crate::spec::{PartitionStatistics, Snapshot};
use crate::table::SnapshotManager;
use crate::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// Interface to commit a snapshot atomically.
///
/// Two implementations:
/// - `RenamingSnapshotCommit` — file system atomic rename
/// - `RESTSnapshotCommit` — via Catalog API (e.g. REST)
#[async_trait]
pub trait SnapshotCommit: Send + Sync {
    /// Commit the given snapshot. Returns true if successful, false if
    /// another writer won the race.
    async fn commit(&self, snapshot: &Snapshot, statistics: &[PartitionStatistics])
        -> Result<bool>;
}

/// A SnapshotCommit using file renaming to commit.
///
/// Reference: [pypaimon RenamingSnapshotCommit](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/snapshot/renaming_snapshot_commit.py)
pub struct RenamingSnapshotCommit {
    snapshot_manager: SnapshotManager,
}

impl RenamingSnapshotCommit {
    pub fn new(snapshot_manager: SnapshotManager) -> Self {
        Self { snapshot_manager }
    }
}

#[async_trait]
impl SnapshotCommit for RenamingSnapshotCommit {
    async fn commit(
        &self,
        snapshot: &Snapshot,
        _statistics: &[PartitionStatistics],
    ) -> Result<bool> {
        // statistics are not used in file system mode (same as Python)
        self.snapshot_manager.commit_snapshot(snapshot).await
    }
}

/// A SnapshotCommit using REST API to commit (e.g. REST Catalog).
///
/// Reference: [pypaimon RESTSnapshotCommit](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/snapshot/catalog_snapshot_commit.py)
pub struct RESTSnapshotCommit {
    api: Arc<RESTApi>,
    identifier: Identifier,
    uuid: String,
}

impl RESTSnapshotCommit {
    pub fn new(api: Arc<RESTApi>, identifier: Identifier, uuid: String) -> Self {
        Self {
            api,
            identifier,
            uuid,
        }
    }
}

#[async_trait]
impl SnapshotCommit for RESTSnapshotCommit {
    async fn commit(
        &self,
        snapshot: &Snapshot,
        statistics: &[PartitionStatistics],
    ) -> Result<bool> {
        self.api
            .commit_snapshot(&self.identifier, &self.uuid, snapshot, statistics)
            .await
    }
}
