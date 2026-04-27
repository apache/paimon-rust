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

//! REST environment for creating RESTSnapshotCommit instances.

use crate::api::rest_api::RESTApi;
use crate::catalog::Identifier;
use crate::table::snapshot_commit::{RESTSnapshotCommit, SnapshotCommit};
use std::sync::Arc;

/// REST environment that holds the REST API client, identifier, and uuid
/// needed to create a `RESTSnapshotCommit`.
#[derive(Clone)]
pub struct RESTEnv {
    identifier: Identifier,
    uuid: String,
    api: Arc<RESTApi>,
}

impl std::fmt::Debug for RESTEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RESTEnv")
            .field("identifier", &self.identifier)
            .field("uuid", &self.uuid)
            .finish()
    }
}

impl RESTEnv {
    /// Create a new RESTEnv.
    pub fn new(identifier: Identifier, uuid: String, api: Arc<RESTApi>) -> Self {
        Self {
            identifier,
            uuid,
            api,
        }
    }

    /// Get the REST API client.
    pub fn api(&self) -> &Arc<RESTApi> {
        &self.api
    }

    /// Get the table identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Create a `RESTSnapshotCommit` from this environment.
    pub fn snapshot_commit(&self) -> Arc<dyn SnapshotCommit> {
        Arc::new(RESTSnapshotCommit::new(
            self.api.clone(),
            self.identifier.clone(),
            self.uuid.clone(),
        ))
    }
}
