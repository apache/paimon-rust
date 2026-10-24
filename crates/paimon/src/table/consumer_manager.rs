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
use regex::Regex;
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

    /// Validate a consumer name before reading or mutating its metadata.
    pub fn validate_consumer_id(consumer_id: &str) -> crate::Result<()> {
        if consumer_id.is_empty()
            || matches!(consumer_id, "." | "..")
            || consumer_id.contains(['/', '\\'])
            // OpenDAL trims path boundaries, so trailing whitespace would alias another ID.
            || consumer_id.trim_end() != consumer_id
        {
            return Err(crate::Error::DataInvalid {
                message: "consumer id must be a non-empty name without path separators or trailing whitespace".into(),
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

    /// Delete one consumer's progress. A missing file is a successful no-op.
    pub async fn delete(&self, consumer_id: &str) -> crate::Result<()> {
        Self::validate_consumer_id(consumer_id)?;
        let path = format!(
            "{}/{CONSUMER_DIR}/{CONSUMER_PREFIX}{consumer_id}",
            self.table_path
        );
        self.file_io.delete_file(&path).await
    }

    /// Delete consumer files whose ids fully match the inclusion regex but not the exclusion.
    ///
    /// Patterns use Rust `regex` syntax, including Unicode classes and inline flags, but
    /// not look-around or backreferences. Omitted, empty, or Java-whitespace-only inclusion
    /// defaults to `.*`; the equivalent exclusion means no exclusions. Nonblank patterns
    /// are never trimmed. Both patterns are validated before listing or deleting files.
    /// Only filenames are inspected, so corrupt consumer JSON can also be cleared.
    /// Missing directories are a no-op; other storage errors are propagated. Deletion is
    /// not atomic, and a running consumer can recreate its progress at checkpoint completion.
    pub async fn clear(
        &self,
        including_consumers: Option<&str>,
        excluding_consumers: Option<&str>,
    ) -> crate::Result<()> {
        let including = consumer_pattern(
            including_consumers
                .filter(|pattern| !is_java_blank(pattern))
                .unwrap_or(".*"),
            "including_consumers",
        )?;
        let excluding = excluding_consumers
            .filter(|pattern| !is_java_blank(pattern))
            .map(|pattern| consumer_pattern(pattern, "excluding_consumers"))
            .transpose()?;

        let directory = format!("{}/{CONSUMER_DIR}", self.table_path);
        let statuses = match self.file_io.list_status(&directory).await {
            Ok(statuses) => statuses,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let mut paths = Vec::new();
        for status in statuses {
            if status.is_dir {
                continue;
            }
            let name = status.path.rsplit('/').next().unwrap_or(&status.path);
            let Some(id) = name.strip_prefix(CONSUMER_PREFIX) else {
                continue;
            };
            if !including.is_match(id)
                || excluding
                    .as_ref()
                    .is_some_and(|pattern| pattern.is_match(id))
            {
                continue;
            }
            // OpenDAL would trim this path and could delete an excluded consumer instead.
            if status.path.trim_end() != status.path {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Cannot clear consumer '{id}': trailing whitespace in filename"
                    ),
                    source: None,
                });
            }
            paths.push(status.path);
        }
        for path in paths {
            self.file_io.delete_file(&path).await?;
        }
        Ok(())
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

fn consumer_pattern(pattern: &str, argument: &str) -> crate::Result<Regex> {
    let invalid = |error: regex::Error| crate::Error::DataInvalid {
        message: format!("Invalid {argument} regex: {error}"),
        source: Some(Box::new(error)),
    };
    // Validate independently so unmatched groups cannot escape the boundary wrapper.
    Regex::new(pattern).map_err(invalid)?;
    Regex::new(&format!(r"\A(?:{pattern})\z"))
        // A trailing verbose-mode comment can otherwise swallow the closing wrapper.
        .or_else(|_| Regex::new(&format!("\\A(?:{pattern}\n)\\z")))
        .map_err(invalid)
}

fn is_java_blank(value: &str) -> bool {
    // Character.isWhitespace excludes nonbreaking spaces, unlike Rust's trim().
    value.chars().all(|c| {
        matches!(
            c,
            '\u{0009}'..='\u{000d}'
                | '\u{001c}'..='\u{0020}'
                | '\u{1680}'
                | '\u{2000}'..='\u{2006}'
                | '\u{2008}'..='\u{200a}'
                | '\u{2028}'..='\u{2029}'
                | '\u{205f}'
                | '\u{3000}'
        )
    })
}

#[derive(Deserialize, Serialize)]
struct Consumer {
    #[serde(rename = "nextSnapshot")]
    next_snapshot: i64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::io::{FileIOBuilder, FileIOProvider};

    #[tokio::test]
    async fn clear_matches_filenames_without_reading_json_and_preserves_other_branches() {
        for scheme in ["memory", "file"] {
            let tmp = tempfile::tempdir().unwrap();
            let table_path = if scheme == "file" {
                format!("file://{}/table", tmp.path().display())
            } else {
                "memory:/consumer-clear".to_string()
            };
            let file_io = FileIOBuilder::new(scheme).build().unwrap();
            let manager = ConsumerManager::new(file_io.clone(), table_path.clone());
            let branch = manager.with_branch("dev");
            let sibling = manager.with_branch("release");
            for scoped in [&manager, &branch, &sibling] {
                scoped.reset("job-old", 2).await.unwrap();
                scoped.reset("job-keep", 3).await.unwrap();
            }
            let directory = format!("{table_path}/consumer");
            for name in [
                "consumer-job-old",
                "consumer-job-keep",
                "consumer-job-keep-extra",
            ] {
                file_io
                    .new_output(&format!("{directory}/{name}"))
                    .unwrap()
                    .write("{".into())
                    .await
                    .unwrap();
            }
            for name in ["notes", "consumer-job", "consumer-pre-job-old"] {
                file_io
                    .new_output(&format!("{directory}/{name}"))
                    .unwrap()
                    .write("keep".into())
                    .await
                    .unwrap();
            }
            let nested = format!("{directory}/consumer-job-directory");
            file_io.mkdirs(&nested).await.unwrap();
            file_io
                .new_output(&format!("{nested}/consumer-job-child"))
                .unwrap()
                .write("keep child".into())
                .await
                .unwrap();

            branch
                .clear(Some("job-.*"), Some("job-keep"))
                .await
                .unwrap();
            assert_eq!(branch.get("job-old").await.unwrap(), None);
            assert_eq!(branch.get("job-keep").await.unwrap(), Some(3));
            assert!(file_io
                .new_input(&format!("{directory}/consumer-job-old"))
                .unwrap()
                .exists()
                .await
                .unwrap());
            for _ in 0..2 {
                manager
                    .clear(Some("job-.*"), Some("job-keep"))
                    .await
                    .unwrap();
                for name in ["consumer-job-old", "consumer-job-keep-extra"] {
                    assert!(!file_io
                        .new_input(&format!("{directory}/{name}"))
                        .unwrap()
                        .exists()
                        .await
                        .unwrap());
                }
                for (name, expected) in [
                    ("consumer-job-keep", "{"),
                    ("notes", "keep"),
                    ("consumer-job", "keep"),
                    ("consumer-pre-job-old", "keep"),
                    ("consumer-job-directory/consumer-job-child", "keep child"),
                ] {
                    assert_eq!(
                        file_io
                            .new_input(&format!("{directory}/{name}"))
                            .unwrap()
                            .read()
                            .await
                            .unwrap()
                            .as_ref(),
                        expected.as_bytes()
                    );
                }
                assert_eq!(branch.get("job-keep").await.unwrap(), Some(3));
                assert_eq!(sibling.get("job-old").await.unwrap(), Some(2));
                assert_eq!(sibling.get("job-keep").await.unwrap(), Some(3));
            }
        }
    }

    #[tokio::test]
    async fn clear_defaults_and_empty_directories_are_idempotent() {
        for scheme in ["memory", "file"] {
            let tmp = tempfile::tempdir().unwrap();
            let table_path = if scheme == "file" {
                format!("file://{}/table", tmp.path().display())
            } else {
                "memory:/consumer-clear-defaults".to_string()
            };
            let file_io = FileIOBuilder::new(scheme).build().unwrap();
            let manager = ConsumerManager::new(file_io.clone(), table_path.clone());
            manager.clear(None, None).await.unwrap();
            assert!(!file_io
                .exists_dir(&format!("{table_path}/consumer"))
                .await
                .unwrap());
            for blank in [None, Some(""), Some(" \t\r\n"), Some("\u{001c}\u{3000}")] {
                manager.reset("job", 1).await.unwrap();
                manager.clear(blank, Some("job")).await.unwrap();
                assert_eq!(manager.get("job").await.unwrap(), Some(1));
                manager.clear(blank, blank).await.unwrap();
                assert_eq!(manager.get("job").await.unwrap(), None);
                manager.clear(blank, blank).await.unwrap();
            }
            manager.reset("job", 1).await.unwrap();
            for nonblank in [
                "\u{0085}", "\u{00a0}", "\u{2007}", "\u{202f}", " job", "job ",
            ] {
                manager.clear(Some(nonblank), None).await.unwrap();
                assert_eq!(manager.get("job").await.unwrap(), Some(1));
            }
            manager.clear(None, Some("\u{00a0}")).await.unwrap();
            assert_eq!(manager.get("job").await.unwrap(), None);
        }
    }

    #[test]
    fn consumer_patterns_match_entire_ids_using_rust_regex_syntax() {
        for (pattern, id, expected) in [
            ("job", "job-old", false),
            ("job", "pre-job", false),
            ("a|ab", "ab", true),
            ("job|other", "job-extra", false),
            ("(?m)^job$", "job\nother", false),
            ("(?s)job.*", "job\nother", true),
            ("(?i)job", "JOB", true),
            ("(?x) job # trailing comment", "job", true),
            ("(?x) job # trailing comment", "job\n", false),
            (r"\w+", "\u{4efb}\u{52a1}", true),
            (r"(?-u:\w+)", "\u{4efb}\u{52a1}", false),
            ("job.*", "job\rold", true),
            (".*", "job\nold", false),
            (" job", " job", true),
        ] {
            let regex = consumer_pattern(pattern, "including_consumers").unwrap();
            assert_eq!(regex.is_match(id), expected, "{pattern:?}: {id:?}");
        }
    }

    #[tokio::test]
    async fn clear_validates_both_patterns_before_deleting_any_files() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let manager = ConsumerManager::new(file_io, "memory:/consumer-clear-invalid".into());
        for pattern in ["[", r"(job)\1", "(?=job)job", ")|(", "\\"] {
            for (including, excluding, argument) in [
                (Some(pattern), None, "including_consumers"),
                (None, Some(pattern), "excluding_consumers"),
                (Some("no-match"), Some(pattern), "excluding_consumers"),
            ] {
                manager.reset("job", 1).await.unwrap();
                let error = manager.clear(including, excluding).await.unwrap_err();
                assert!(error
                    .to_string()
                    .contains(&format!("Invalid {argument} regex")));
                assert_eq!(manager.get("job").await.unwrap(), Some(1));
            }
        }
    }

    #[tokio::test]
    async fn clear_propagates_listing_and_deletion_errors() {
        #[derive(Debug)]
        struct DeniedProvider {
            operator: opendal::Operator,
            deny_listing: bool,
        }

        #[async_trait::async_trait]
        impl FileIOProvider for DeniedProvider {
            async fn create(&self, path: &str) -> crate::Result<(opendal::Operator, String)> {
                if self.deny_listing || path.ends_with("/consumer-job") {
                    return Err(opendal::Error::new(
                        opendal::ErrorKind::PermissionDenied,
                        "access denied",
                    )
                    .into());
                }
                Ok((
                    self.operator.clone(),
                    path.strip_prefix("memory:/").unwrap().to_string(),
                ))
            }
        }

        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        operator
            .write("table/consumer/consumer-job", "{")
            .await
            .unwrap();
        for deny_listing in [true, false] {
            let file_io = FileIOBuilder::new("memory")
                .build()
                .unwrap()
                .with_provider(Arc::new(DeniedProvider {
                    operator: operator.clone(),
                    deny_listing,
                }));
            let manager = ConsumerManager::new(file_io, "memory:/table".into());
            let error = manager.clear(None, None).await.unwrap_err();
            assert!(matches!(error, crate::Error::IoUnexpected { source, .. }
                if source.kind() == opendal::ErrorKind::PermissionDenied));
            assert!(operator
                .exists("table/consumer/consumer-job")
                .await
                .unwrap());
        }
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn clear_uses_listed_paths_without_aliasing_excluded_consumers() {
        let tmp = tempfile::tempdir().unwrap();
        let directory = tmp.path().join("consumer");
        std::fs::create_dir(&directory).unwrap();
        let manager = ConsumerManager::new(
            FileIOBuilder::new("file").build().unwrap(),
            format!("file://{}", tmp.path().display()),
        );
        std::fs::write(directory.join(r"consumer-job\part"), "{").unwrap();
        manager.clear(Some(r"job\\part"), None).await.unwrap();
        assert!(!directory.join(r"consumer-job\part").exists());

        std::fs::write(directory.join("consumer-job"), "keep").unwrap();
        for suffix in [" ", "\t", "\u{00a0}", "\u{3000}"] {
            let name = format!("consumer-job{suffix}");
            std::fs::write(directory.join(&name), "keep alias").unwrap();
            // The filesystem lister normalizes these names to "consumer-job".
            manager
                .clear(Some(&format!("job{suffix}")), Some("job"))
                .await
                .unwrap();
            assert_eq!(
                std::fs::read(directory.join("consumer-job")).unwrap(),
                b"keep"
            );
            assert_eq!(std::fs::read(directory.join(&name)).unwrap(), b"keep alias");
        }
        manager.clear(None, Some("job")).await.unwrap();
        assert_eq!(
            std::fs::read(directory.join("consumer-job")).unwrap(),
            b"keep"
        );
    }

    #[tokio::test]
    async fn clear_rejects_literal_whitespace_paths_before_any_deletion() {
        use opendal::raw::{oio::Write, OpWrite, Service};

        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        let file_io = FileIOBuilder::new("file")
            .with_fs_operator(operator.clone())
            .build()
            .unwrap();
        let manager = ConsumerManager::new(file_io, "file:///table".into());
        operator
            .write("table/consumer/consumer-job", "keep")
            .await
            .unwrap();
        operator
            .write("table/consumer/consumer-delete", "keep")
            .await
            .unwrap();
        for suffix in [" ", "\t", "\u{00a0}", "\u{3000}"] {
            let path = format!("table/consumer/consumer-job{suffix}");
            // Seed a literal object key without Operator's input path normalization.
            let mut writer = operator
                .service()
                .write(operator.context(), &path, OpWrite::default())
                .unwrap();
            writer.write("keep alias".into()).await.unwrap();
            writer.close().await.unwrap();
            let error = manager
                .clear(Some(&format!("job{suffix}")), Some("job"))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("trailing whitespace"));
            assert!(manager.clear(None, Some("job")).await.is_err());
            assert_eq!(
                operator
                    .read("table/consumer/consumer-job")
                    .await
                    .unwrap()
                    .to_bytes(),
                "keep"
            );
            assert_eq!(
                operator
                    .read("table/consumer/consumer-delete")
                    .await
                    .unwrap()
                    .to_bytes(),
                "keep"
            );
            assert!(operator
                .list("table/consumer/")
                .await
                .unwrap()
                .iter()
                .any(|entry| entry.path() == path));
        }
    }

    #[tokio::test]
    async fn delete_is_idempotent_and_preserves_other_consumers_and_branches() {
        for scheme in ["memory", "file"] {
            let tmp = tempfile::tempdir().unwrap();
            let table_path = if scheme == "file" {
                format!("file://{}/table", tmp.path().display())
            } else {
                "memory:/consumer-delete".to_string()
            };
            let file_io = FileIOBuilder::new(scheme).build().unwrap();
            let manager = ConsumerManager::new(file_io.clone(), table_path.clone());
            let branch = manager.with_branch("dev");
            let sibling = manager.with_branch("release");
            manager.delete("missing").await.unwrap();
            assert!(!file_io
                .exists_dir(&format!("{table_path}/consumer"))
                .await
                .unwrap());
            manager.reset("job", 1).await.unwrap();
            manager.reset("other", 2).await.unwrap();
            branch.reset("job", 3).await.unwrap();
            sibling.reset("job", 4).await.unwrap();

            branch.delete("job").await.unwrap();
            branch.delete("job").await.unwrap();
            assert_eq!(branch.get("job").await.unwrap(), None);
            assert_eq!(manager.get("job").await.unwrap(), Some(1));
            assert_eq!(sibling.get("job").await.unwrap(), Some(4));
            assert!(file_io
                .exists_dir(&format!("{table_path}/branch/branch-dev/consumer"))
                .await
                .unwrap());

            branch.reset("job", 5).await.unwrap();
            manager.delete("job").await.unwrap();
            manager.delete("job").await.unwrap();
            assert_eq!(
                manager.list_all().await.unwrap(),
                vec![("other".to_string(), 2)]
            );
            assert_eq!(branch.get("job").await.unwrap(), Some(5));
            assert_eq!(sibling.get("job").await.unwrap(), Some(4));
        }
    }

    #[tokio::test]
    async fn trailing_whitespace_cannot_alias_another_consumers_progress() {
        for scheme in ["memory", "file"] {
            let tmp = tempfile::tempdir().unwrap();
            let table_path = if scheme == "file" {
                format!("file://{}/table", tmp.path().display())
            } else {
                "memory:/consumer-whitespace".to_string()
            };
            let file_io = FileIOBuilder::new(scheme).build().unwrap();
            let manager = ConsumerManager::new(file_io.clone(), table_path);
            let branch = manager.with_branch("dev");
            for scoped in [&manager, &branch] {
                scoped.reset("job", 1).await.unwrap();
                let path = format!("{}/consumer/consumer-job", scoped.table_path);
                let original = file_io.new_input(&path).unwrap().read().await.unwrap();
                for id in [
                    "job ",
                    "job\t",
                    "job\n",
                    "job\r\n",
                    "job\u{a0}",
                    "job\u{3000}",
                ] {
                    assert_eq!(scoped.get(id).await.unwrap(), None);
                    assert!(scoped.delete(id).await.is_err(), "{scheme}: {id:?}");
                    assert_eq!(
                        file_io.new_input(&path).unwrap().read().await.unwrap(),
                        original
                    );
                    assert!(scoped.reset(id, 3).await.is_err(), "{scheme}: {id:?}");
                    assert_eq!(
                        file_io.new_input(&path).unwrap().read().await.unwrap(),
                        original
                    );
                }
                scoped.reset(" job", 2).await.unwrap();
                assert_eq!(scoped.get(" job").await.unwrap(), Some(2));
                scoped.delete(" job").await.unwrap();
                assert_eq!(scoped.get("job").await.unwrap(), Some(1));
            }
        }
    }

    #[tokio::test]
    async fn delete_rejects_invalid_ids_without_changing_files() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let manager =
            ConsumerManager::new(file_io.clone(), "memory:/consumer-delete-invalid".into());
        manager.reset("job", 2).await.unwrap();
        for invalid in ["", ".", "..", "x/../../snapshot/snapshot-1", "x\\y"] {
            assert!(manager.delete(invalid).await.is_err());
        }
        assert_eq!(
            manager.list_all().await.unwrap(),
            vec![("job".to_string(), 2)]
        );
    }

    #[tokio::test]
    async fn delete_propagates_storage_errors() {
        #[derive(Debug)]
        struct DeniedProvider;

        #[async_trait::async_trait]
        impl FileIOProvider for DeniedProvider {
            async fn create(&self, _path: &str) -> crate::Result<(opendal::Operator, String)> {
                Err(
                    opendal::Error::new(opendal::ErrorKind::PermissionDenied, "access denied")
                        .into(),
                )
            }
        }

        let file_io = FileIOBuilder::new("memory")
            .build()
            .unwrap()
            .with_provider(Arc::new(DeniedProvider));
        let manager = ConsumerManager::new(file_io, "memory:/consumer-delete-denied".into());
        let error = manager.delete("job").await.unwrap_err();
        assert!(matches!(error, crate::Error::IoUnexpected { source, .. }
            if source.kind() == opendal::ErrorKind::PermissionDenied));
    }

    #[tokio::test]
    async fn delete_does_not_recursively_remove_a_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let directory = tmp.path().join("consumer/consumer-job");
        std::fs::create_dir_all(&directory).unwrap();
        let child = directory.join("keep");
        std::fs::write(&child, b"keep").unwrap();
        let manager = ConsumerManager::new(
            FileIOBuilder::new("file").build().unwrap(),
            format!("file://{}", tmp.path().display()),
        );
        assert!(manager.delete("job").await.is_err());
        assert_eq!(std::fs::read(child).unwrap(), b"keep");
    }

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
