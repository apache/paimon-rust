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

use super::*;
use crate::table::global_index_build_common::GlobalIndexBuildView;
use crate::table::global_index_options::{definitions, Definition};
use sha2::{Digest, Sha256};

mod planning;

impl TableCommit {
    pub(super) async fn auto_index_enabled(&self, has_new_files: bool) -> Result<bool> {
        if !has_new_files
            || !self.table.schema().primary_keys().is_empty()
            || self.table.schema().core_options().write_only()?
        {
            return Ok(false);
        }
        let definitions = definitions(&self.table)?;
        for definition in &definitions {
            definition.builder(&self.table).validate().await?;
        }
        Ok(!definitions.is_empty())
    }

    fn auto_index_user(&self, written: &Snapshot) -> String {
        let mut identity = Sha256::new();
        identity.update(self.table.location().as_bytes());
        identity.update(self.table.branch().as_bytes());
        identity.update(written.delta_manifest_list().as_bytes());
        format!(
            "{}-auto-index-build-{}",
            self.commit_user,
            hex::encode(identity.finalize())
        )
    }

    pub(super) async fn index_after_write(&self, written: &Snapshot) -> Result<()> {
        self.try_index_after_write(written)
            .await
            .map_err(|error| auto_index_error(written.commit_identifier(), error))
    }

    async fn try_index_after_write(&self, written: &Snapshot) -> Result<()> {
        let commit_user = self.auto_index_user(written);
        let start = current_time_millis();
        let mut retries = 0u64;
        let mut publication_error = None;
        loop {
            let latest = self.snapshot_manager.get_latest_snapshot().await?;
            let snapshot = latest
                .as_ref()
                .filter(|s| s.id() >= written.id())
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "Cannot build automatic indexes after rollback below the data commit"
                        .into(),
                    source: None,
                })?;
            let table = self.post_commit_table(Some(snapshot)).await?;
            let mut commit = Self::new(table.clone(), commit_user.clone());
            commit.snapshot_commit = self.snapshot_commit.clone();
            commit.snapshot_properties = self.snapshot_properties.clone();
            commit.commit_callbacks = self.commit_callbacks.clone();
            // Check completion before the retry budget or touching old input files.
            // This also recovers a successful publication whose response was lost.
            if commit
                .is_committed_identifier_since(&latest, written.id(), Some(written.id() + 1))
                .await?
            {
                return Ok(());
            }
            if retries > u64::from(self.commit_max_retries)
                || (retries > 0 && current_time_millis() - start > self.commit_timeout_ms)
            {
                return Err(crate::Error::DataInvalid {
                    message: format!("Automatic index build exhausted the commit retry budget after {retries} conflicts"),
                    source: publication_error.map(|error| Box::new(error) as _),
                });
            }
            if table.schema().core_options().write_only()? {
                return Ok(());
            }
            let definitions = definitions(&table)?;
            if definitions.is_empty() {
                return Ok(());
            }
            let data: Arc<[ManifestEntry]> =
                crate::table::snapshot_manifest::read_snapshot_entries(
                    &self.snapshot_manager,
                    snapshot,
                    None,
                )
                .await?
                .into();
            let view = GlobalIndexBuildView::capture(&table, Some(snapshot.clone()))
                .await?
                .with_source_entries(data.clone());
            let mut messages = Vec::new();
            let prepared: Result<()> = async {
                for definition in definitions {
                    let mut plan =
                        planning::plan(&table, &definition, &data, view.index_entries()).await?;
                    if plan.needs_build {
                        let built = match definition
                            .builder(&table)
                            .build_with_view(plan.build_view(&view))
                            .await
                        {
                            Ok(built) => built,
                            Err(error) => {
                                let expandable =
                                    crate::table::index_build::vector::training_range(&error)
                                        .is_some_and(|range| {
                                            plan.can_expand_training_range(&range)
                                        });
                                if !expandable {
                                    return Err(error);
                                }
                                // A small/sparse leaf may not meet a persisted-size
                                // objective. Retry once with larger pending groups;
                                // keep the same codec, metric and source snapshot.
                                plan = planning::plan_for_training(
                                    &table,
                                    &definition,
                                    &data,
                                    view.index_entries(),
                                )
                                .await?;
                                definition
                                    .builder(&table)
                                    .build_with_view(plan.build_view(&view))
                                    .await?
                            }
                        };
                        messages.extend(built);
                    } else {
                        // Preserve validation for a refreshed schema without
                        // planning already covered shards.
                        definition.builder(&table).validate().await?;
                    }
                    messages.extend(plan.deletions);
                }
                Ok(())
            }
            .await;
            if let Err(error) = prepared {
                // No files in this round have been submitted to a publisher.
                // Abort only generated outputs; retired payloads remain readable.
                let _ = commit.abort(&messages).await;
                return Err(error);
            }
            if messages.is_empty() {
                return Ok(());
            }
            let current = self.snapshot_manager.get_latest_snapshot().await?;
            if !current
                .as_ref()
                .is_some_and(|s| s.has_same_read_metadata(snapshot))
                || self.latest_schema_id().await? != table.schema().id()
            {
                let _ = commit.abort(&messages).await;
            } else {
                let mut plan = CommitEntriesPlan::Direct {
                    entries: vec![],
                    changelog_entries: vec![],
                    new_index_entries: commit.messages_to_index_entries(&messages),
                    check_from_snapshot: None,
                };
                let resolved = match commit.resolve_commit(&mut plan, &latest, None, false).await {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        let _ = commit.abort(&messages).await;
                        return Err(error);
                    }
                };
                // One atomic attempt: a definite CAS conflict requires a fresh
                // coverage plan, rather than rebasing overlapping index files.
                match commit
                    .try_commit_once(resolved, &latest, written.id())
                    .await?
                {
                    CommitAttemptResult::Success(_) => return Ok(()),
                    CommitAttemptResult::Retry(state) => match state.publication_error {
                        None => {
                            let _ = commit.abort(&messages).await;
                        }
                        Some(error) => {
                            // Publication is uncertain. Retain every submitted
                            // file, and probe the stable identity on the next pass.
                            publication_error = Some(error);
                        }
                    },
                }
            }
            self.commit_retry_wait(retries.min(u64::from(u32::MAX)) as u32)
                .await;
            retries += 1;
        }
    }
}

pub(super) fn auto_index_error(identifier: i64, error: crate::Error) -> crate::Error {
    crate::Error::UnexpectedError {
        message: format!("Data commit {identifier} succeeded, but automatic index build failed; retry through the corresponding recovery API with the same commit user and identifier"),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::table_commit::auto_index_tests::{assert_complete, latest, messages, table};
    #[tokio::test]
    async fn builders_only_plan_sources_overlapping_changed_units() {
        use crate::table::global_index_options::definitions;
        let table = table(Some(true), false).await;
        TableCommit::new(table.clone(), "initial".into())
            .commit(messages(&table, vec![1, 2]).await)
            .await
            .unwrap();
        let raw = table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
        TableCommit::new(raw.clone(), "append".into())
            .commit(messages(&raw, vec![3, 4]).await)
            .await
            .unwrap();
        let snapshot = latest(&table).await;
        let data: Arc<[ManifestEntry]> = crate::table::snapshot_manifest::read_snapshot_entries(
            &table.snapshot_manager(),
            &snapshot,
            None,
        )
        .await
        .unwrap()
        .into();
        assert_eq!(data.len(), 2);
        let view = GlobalIndexBuildView::capture(&table, Some(snapshot))
            .await
            .unwrap()
            .with_source_entries(data.clone());
        let mut messages = Vec::new();
        for definition in definitions(&table).unwrap() {
            let plan = planning::plan(&table, &definition, &data, view.index_entries())
                .await
                .unwrap();
            assert!(plan.needs_build);
            let build_view = plan.build_view(&view);
            let sources = build_view.manifest_entries(&table, None).await.unwrap();
            assert_eq!(
                sources.len(),
                1,
                "covered units must not enter per-column shard planning"
            );
            assert_eq!(sources[0].file().first_row_id, Some(2));
            messages.extend(
                definition
                    .builder(&table)
                    .build_with_view(build_view)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(messages.len(), 2);
        TableCommit::new(table.clone(), "build-new".into())
            .commit(messages)
            .await
            .unwrap();
        assert_complete(&table).await;
    }
}
