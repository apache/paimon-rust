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
use futures::future::try_join_all;
use opendal::raw::get_basename;

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

    /// List all tag names sorted ascending. Returns an empty vector when the
    /// tag directory does not exist.
    pub async fn list_all_names(&self) -> crate::Result<Vec<String>> {
        let tag_dir = self.tag_directory();
        let statuses = match self.file_io.list_status(&tag_dir).await {
            Ok(s) => s,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        let mut names: Vec<String> = statuses
            .into_iter()
            .filter(|s| !s.is_dir)
            .filter_map(|s| {
                get_basename(&s.path)
                    .strip_prefix(TAG_PREFIX)
                    .map(str::to_string)
            })
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    /// Create a tag by writing the snapshot JSON to the tag path.
    pub async fn create(&self, tag_name: &str, snapshot: &Snapshot) -> crate::Result<()> {
        let path = self.tag_path(tag_name);
        let json = serde_json::to_string(snapshot).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to serialize snapshot for tag '{tag_name}': {e}"),
            source: Some(Box::new(e)),
        })?;
        let output = self.file_io.new_output(&path)?;
        output.write(bytes::Bytes::from(json)).await
    }

    /// Delete a tag file.
    pub async fn delete(&self, tag_name: &str) -> crate::Result<()> {
        let path = self.tag_path(tag_name);
        self.file_io.delete_file(&path).await
    }

    /// List all tags as `(name, snapshot)` pairs sorted by name ascending.
    pub async fn list_all(&self) -> crate::Result<Vec<(String, Snapshot)>> {
        let names = self.list_all_names().await?;
        try_join_all(names.into_iter().map(|name| async move {
            let snap = self
                .get(&name)
                .await?
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("tag '{name}' disappeared during listing"),
                    source: None,
                })?;
            Ok::<_, crate::Error>((name, snap))
        }))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::CommitKind;
    use bytes::Bytes;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_snapshot(id: i64) -> Snapshot {
        Snapshot::builder()
            .version(3)
            .id(id)
            .schema_id(0)
            .base_manifest_list("base-list".to_string())
            .delta_manifest_list("delta-list".to_string())
            .commit_user("test-user".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000 * id as u64)
            .build()
    }

    async fn write_tag(file_io: &FileIO, tm: &TagManager, name: &str, snapshot: &Snapshot) {
        let path = tm.tag_path(name);
        let json = serde_json::to_string(snapshot).unwrap();
        let output = file_io.new_output(&path).unwrap();
        output.write(Bytes::from(json)).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_all_names_missing_dir_returns_empty() {
        let file_io = test_file_io();
        let tm = TagManager::new(file_io, "memory:/test_tag_missing".to_string());
        assert!(tm.list_all_names().await.unwrap().is_empty());
        assert!(tm.list_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_all_names_sorted() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_sorted".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);
        for name in ["v3", "v1", "v2"] {
            write_tag(&file_io, &tm, name, &test_snapshot(1)).await;
        }
        assert_eq!(tm.list_all_names().await.unwrap(), vec!["v1", "v2", "v3"]);
    }

    #[tokio::test]
    async fn test_list_all_loads_pairs() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_pairs".to_string();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path);
        write_tag(&file_io, &tm, "a", &test_snapshot(1)).await;
        write_tag(&file_io, &tm, "b", &test_snapshot(2)).await;
        let pairs = tm.list_all().await.unwrap();
        let names: Vec<&str> = pairs.iter().map(|(n, _)| n.as_str()).collect();
        let ids: Vec<i64> = pairs.iter().map(|(_, s)| s.id()).collect();
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(ids, vec![1, 2]);
    }
}
