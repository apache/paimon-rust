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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter};

/// Deserialize `_NULL_COUNTS` which in Avro is `["null", {"type":"array","items":["null","long"]}]`.
/// Preserves null items as `None` (meaning "unknown") rather than collapsing to 0.
fn deserialize_null_counts<'de, D>(deserializer: D) -> Result<Vec<Option<i64>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<Vec<Option<i64>>> = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

fn serialize_null_counts<S>(value: &[Option<i64>], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Serialize as Option<Vec<Option<i64>>> to match the Avro union schema.
    let wrapped: Option<&[Option<i64>]> = Some(value);
    wrapped.serialize(serializer)
}

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
    #[serde(
        rename = "_NULL_COUNTS",
        deserialize_with = "deserialize_null_counts",
        serialize_with = "serialize_null_counts"
    )]
    null_counts: Vec<Option<i64>>,
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
    pub fn null_counts(&self) -> &Vec<Option<i64>> {
        &self.null_counts
    }

    pub fn new(
        min_values: Vec<u8>,
        max_values: Vec<u8>,
        null_counts: Vec<Option<i64>>,
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
