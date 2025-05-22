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

use indexmap::IndexMap;

/// Metadata of index file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/index/IndexFileMeta.java>
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFileMeta {
    #[serde(rename = "_INDEX_TYPE")]
    pub index_type: String,

    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,

    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i32,

    #[serde(rename = "_ROW_COUNT")]
    pub row_count: i32,

    // use Indexmap to ensure the order of deletion_vectors_ranges is consistent.
    #[serde(
        default,
        with = "map_serde",
        rename = "_DELETIONS_VECTORS_RANGES",
        alias = "_DELETION_VECTORS_RANGES"
    )]
    pub deletion_vectors_ranges: Option<IndexMap<String, (i32, i32)>>,
}

impl Display for IndexFileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexFileMeta{{index_type={}, fileName={}, fileSize={}, rowCount={}, deletion_vectors_ranges={:?}}}",
            self.index_type,
            self.file_name,
            self.file_size,
            self.row_count,
            self.deletion_vectors_ranges,
        )
    }
}

mod map_serde {
    use indexmap::IndexMap;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Deserialize, Serialize)]
    struct Temp {
        f0: String,
        f1: i32,
        f2: i32,
    }

    pub fn serialize<S>(
        date: &Option<IndexMap<String, (i32, i32)>>,
        s: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *date {
            None => s.serialize_none(),
            Some(ref d) => s.collect_seq(d.iter().map(|(s, (i1, i2))| Temp {
                f0: s.into(),
                f1: *i1,
                f2: *i2,
            })),
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<IndexMap<String, (i32, i32)>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::deserialize(deserializer)? {
            None => Ok(None),
            Some::<Vec<Temp>>(s) => Ok(Some(
                s.into_iter()
                    .map(|t| (t.f0, (t.f1, t.f2)))
                    .collect::<IndexMap<_, _>>(),
            )),
        }
    }
}
