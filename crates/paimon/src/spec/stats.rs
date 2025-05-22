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

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// The statistics for columns, supports the following stats.
///
/// All statistics are stored in the form of a Binary, which can significantly reduce its memory consumption, but the cost is that the column type needs to be known when getting.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/stats/FieldStatsArraySerializer.java#L111>
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BinaryTableStats {
    /// the minimum values of the columns
    #[serde(rename = "_MIN_VALUES", with = "serde_bytes")]
    min_values: Vec<u8>,

    /// the maximum values of the columns
    #[serde(rename = "_MAX_VALUES", with = "serde_bytes")]
    max_values: Vec<u8>,

    /// the number of nulls of the columns
    #[serde(rename = "_NULL_COUNTS")]
    null_counts: Vec<i64>,
}

impl BinaryTableStats {
    /// Get the minimum values of the columns
    #[inline]
    pub fn min_values(&self) -> &[u8] {
        &self.min_values
    }

    /// Get the maximum values of the columns
    #[inline]
    pub fn max_values(&self) -> &[u8] {
        &self.max_values
    }

    /// Get the number of nulls of the columns
    #[inline]
    pub fn null_counts(&self) -> &Vec<i64> {
        &self.null_counts
    }

    pub fn new(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<i64>,
    ) -> BinaryTableStats {
        Self {
            min_values,
            max_values,
            null_counts,
        }
    }
}

impl Display for BinaryTableStats {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
