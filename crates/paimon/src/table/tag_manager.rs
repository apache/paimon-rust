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

//! Tag manager for reading tag metadata using FileIO.
//!
//! Reference: [org.apache.paimon.utils.TagManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java)
//! and [pypaimon.tag.tag_manager.TagManager](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/tag/tag_manager.py).

use crate::io::FileIO;
use crate::spec::Snapshot;

const TAG_DIR: &str = "tag";
const TAG_PREFIX: &str = "tag-";

/// Manager for tag files using unified FileIO.
///
/// Tags are named snapshots stored as JSON files at `{table_path}/tag/tag-{name}`.
/// The tag file format is identical to a Snapshot JSON file.
///
/// Reference: [org.apache.paimon.utils.TagManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java)
#[derive(Debug, Clone)]
pub struct TagManager {
    file_io: FileIO,
    table_path: String,
}

impl TagManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
        }
    }

    /// Path to the tag directory (e.g. `table_path/tag`).
    pub fn tag_directory(&self) -> String {
        format!("{}/{}", self.table_path, TAG_DIR)
    }

    /// Path to the tag file for the given name (e.g. `tag/tag-my_tag`).
    pub fn tag_path(&self, tag_name: &str) -> String {
        format!("{}/{}{}", self.tag_directory(), TAG_PREFIX, tag_name)
    }

    /// Check if a tag exists.
    pub async fn tag_exists(&self, tag_name: &str) -> crate::Result<bool> {
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        input.exists().await
    }

    /// Get the snapshot for a tag, or None if the tag file does not exist.
    ///
    /// Tag files are JSON with the same schema as Snapshot.
    /// Reads directly and catches NotFound to avoid a separate exists() IO round-trip.
    pub async fn get(&self, tag_name: &str) -> crate::Result<Option<Snapshot>> {
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        let bytes = match input.read().await {
            Ok(b) => b,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let snapshot: Snapshot =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("tag '{tag_name}' JSON invalid: {e}"),
                source: Some(Box::new(e)),
            })?;
        Ok(Some(snapshot))
    }
}
