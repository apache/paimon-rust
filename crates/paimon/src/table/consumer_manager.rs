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

//! Consumer progress manager for Java-compatible consumer files.

use std::collections::HashSet;
use std::time::Duration;

use crate::io::FileIO;
use futures::{stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

const CONSUMER_DIR: &str = "consumer";
const CONSUMER_PREFIX: &str = "consumer-";
const CONSUMER_READ_CONCURRENCY: usize = 32;
const READ_RETRIES: usize = 10;
const RETRY_DELAY: Duration = Duration::from_millis(200);

/// Reads consumer progress files stored under a table or branch.
#[derive(Debug, Clone)]
pub struct ConsumerManager {
    file_io: FileIO,
    table_path: String,
}

impl ConsumerManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
        }
    }

    /// Create a manager for a branch of this table.
    pub fn with_branch(&self, branch_name: &str) -> Self {
        Self::new(
            self.file_io.clone(),
            format!("{}/branch/branch-{branch_name}", self.table_path),
        )
    }

    pub(super) fn validate_consumer_id(consumer_id: &str) -> crate::Result<()> {
        if consumer_id.is_empty()
            || matches!(consumer_id, "." | "..")
            || consumer_id.contains(['/', '\\'])
        {
            return Err(crate::Error::DataInvalid {
                message: "consumer id must be a non-empty name without path separators".into(),
                source: None,
            });
        }
        Ok(())
    }

    /// Read one consumer's next snapshot id.
    pub async fn get(&self, consumer_id: &str) -> crate::Result<Option<i64>> {
        let ids = HashSet::from([consumer_id]);
        Ok(self
            .list_matching(Some(&ids))
            .await?
            .into_iter()
            .next()
            .map(|(_, next_snapshot)| next_snapshot))
    }

    /// Persist an explicitly acknowledged next snapshot, like Java resetConsumer.
    pub async fn reset(&self, consumer_id: &str, next_snapshot: i64) -> crate::Result<()> {
        Self::validate_consumer_id(consumer_id)?;
        if next_snapshot < 1 {
            return Err(crate::Error::DataInvalid {
                message: "consumer next snapshot must be positive".into(),
                source: None,
            });
        }
        let directory = format!("{}/{}", self.table_path, CONSUMER_DIR);
        self.file_io.mkdirs(&directory).await?;
        let path = format!("{directory}/{CONSUMER_PREFIX}{consumer_id}");
        let bytes = serde_json::to_vec(&Consumer { next_snapshot }).map_err(|error| {
            crate::Error::DataInvalid {
                message: format!("consumer JSON invalid: {error}"),
                source: Some(Box::new(error)),
            }
        })?;
        self.file_io.new_output(&path)?.write(bytes.into()).await
    }

    async fn read_path(&self, consumer_id: &str, path: &str) -> crate::Result<Option<i64>> {
        let input = self.file_io.new_input(path)?;
        for attempt in 0..READ_RETRIES {
            let bytes = match input.read().await {
                Ok(bytes) => bytes,
                Err(crate::Error::IoUnexpected { ref source, .. })
                    if source.kind() == opendal::ErrorKind::NotFound =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            match serde_json::from_slice::<Consumer>(&bytes) {
                Ok(consumer) => return Ok(Some(consumer.next_snapshot)),
                Err(_) if attempt + 1 < READ_RETRIES => tokio::time::sleep(RETRY_DELAY).await,
                Err(error) => {
                    return Err(crate::Error::DataInvalid {
                        message: format!("consumer '{consumer_id}' JSON invalid: {error}"),
                        source: Some(Box::new(error)),
                    });
                }
            }
        }
        unreachable!("READ_RETRIES is non-zero")
    }

    /// List consumer ids and their next snapshot ids, sorted by consumer id.
    pub async fn list_all(&self) -> crate::Result<Vec<(String, i64)>> {
        self.list_matching(None).await
    }

    /// List the requested consumers that have exactly matching directory entries.
    pub async fn list_by_ids(&self, consumer_ids: &[String]) -> crate::Result<Vec<(String, i64)>> {
        let ids = consumer_ids.iter().map(String::as_str).collect();
        self.list_matching(Some(&ids)).await
    }

    async fn list_matching(
        &self,
        consumer_ids: Option<&HashSet<&str>>,
    ) -> crate::Result<Vec<(String, i64)>> {
        let directory = format!("{}/{}", self.table_path, CONSUMER_DIR);
        let statuses = match self.file_io.list_status(&directory).await {
            Ok(statuses) => statuses,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };

        let mut consumers = stream::iter(statuses.into_iter().filter_map(|status| {
            if status.is_dir {
                return None;
            }
            let name = status.path.rsplit('/').next().unwrap_or(&status.path);
            let id = name.strip_prefix(CONSUMER_PREFIX)?;
            if consumer_ids.is_some_and(|ids| !ids.contains(id)) {
                return None;
            }
            Some((id.to_string(), status.path))
        }))
        .map(|(id, path)| async move {
            self.read_path(&id, &path)
                .await
                .map(|next_snapshot| next_snapshot.map(|next_snapshot| (id, next_snapshot)))
        })
        .buffer_unordered(CONSUMER_READ_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        consumers.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Ok(consumers)
    }
}

#[derive(Deserialize, Serialize)]
struct Consumer {
    #[serde(rename = "nextSnapshot")]
    next_snapshot: i64,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::io::FileIOBuilder;

    #[tokio::test]
    async fn reset_is_explicit_branch_scoped_and_rejects_path_traversal() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let manager = ConsumerManager::new(file_io.clone(), "memory:/consumer-reset".into());
        let branch = manager.with_branch("dev");
        for invalid in ["", ".", "..", "x/../../snapshot/snapshot-1", "x\\y"] {
            assert!(manager.reset(invalid, 2).await.is_err());
        }
        assert!(!file_io
            .new_input("memory:/consumer-reset/snapshot/snapshot-1")
            .unwrap()
            .exists()
            .await
            .unwrap());
        assert!(manager.reset("job", 0).await.is_err());
        branch.reset("job", 2).await.unwrap();
        assert_eq!(branch.get("job").await.unwrap(), Some(2));
        assert_eq!(manager.get("job").await.unwrap(), None);
        branch.reset("job", 3).await.unwrap();
        assert_eq!(branch.get("job").await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn retries_a_consumer_being_overwritten() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/consumer-retry";
        let directory = format!("{table_path}/{CONSUMER_DIR}");
        let path = format!("{directory}/{CONSUMER_PREFIX}job");
        file_io.mkdirs(&directory).await.unwrap();
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"{"))
            .await
            .unwrap();

        let manager = ConsumerManager::new(file_io.clone(), table_path.to_string());
        let mut read = Box::pin(manager.get("job"));
        tokio::select! {
            result = &mut read => panic!("invalid JSON returned before retry: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(br#"{"nextSnapshot":5}"#))
            .await
            .unwrap();

        assert_eq!(read.await.unwrap(), Some(5));
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn lists_posix_consumer_id_with_backslash() {
        let tmp = tempfile::tempdir().unwrap();
        let table_path = tmp.path().join("table");
        let directory = table_path.join(CONSUMER_DIR);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(r"consumer-id\part"), r#"{"nextSnapshot":5}"#).unwrap();

        let manager = ConsumerManager::new(
            FileIOBuilder::new("file").build().unwrap(),
            format!("file://{}", table_path.display()),
        );

        assert_eq!(
            manager.list_all().await.unwrap(),
            vec![(r"id\part".to_string(), 5)]
        );
        assert_eq!(manager.get(r"id\part").await.unwrap(), Some(5));
    }
}
