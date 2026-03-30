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

//! REST token for data access in Apache Paimon.

use std::collections::HashMap;

/// Token for REST data access, containing credentials and expiration.
///
/// Corresponds to Python `RESTToken` in `pypaimon/catalog/rest/rest_token.py`.
#[derive(Debug, Clone)]
pub struct RESTToken {
    /// Token key-value pairs (e.g. access_key_id, access_key_secret, etc.)
    pub token: HashMap<String, String>,
    /// Token expiration time in milliseconds since epoch.
    pub expire_at_millis: i64,
}

impl RESTToken {
    /// Create a new RESTToken.
    pub fn new(token: HashMap<String, String>, expire_at_millis: i64) -> Self {
        Self {
            token,
            expire_at_millis,
        }
    }
}

impl PartialEq for RESTToken {
    fn eq(&self, other: &Self) -> bool {
        self.expire_at_millis == other.expire_at_millis && self.token == other.token
    }
}

impl Eq for RESTToken {}

impl std::hash::Hash for RESTToken {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expire_at_millis.hash(state);
        // Sort keys for deterministic hashing
        let mut pairs: Vec<_> = self.token.iter().collect();
        pairs.sort_by_key(|(k, _)| (*k).clone());
        for (k, v) in pairs {
            k.hash(state);
            v.hash(state);
        }
    }
}
