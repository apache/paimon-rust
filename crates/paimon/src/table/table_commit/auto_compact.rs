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
use sha2::{Digest, Sha256};

impl TableCommit {
    pub(super) fn auto_compaction_enabled(&self, has_new_files: bool) -> Result<bool> {
        if self.table.schema().core_options().write_only()? || !has_new_files {
            return Ok(false);
        }
        // Modes outside the compact API retain their existing write support.
        match crate::table::compact::ensure_supported(&self.table) {
            Ok(()) => {}
            Err(crate::Error::Unsupported { .. }) => return Ok(false),
            Err(error) => return Err(error),
        }
        // Validate planner configuration before publishing data. Executor
        // resources are created only when this write has eligible work.
        let builder = self.table.new_compact_builder();
        builder.new_planner()?;
        self.validate_auto_compaction_policy()?;
        Ok(true)
    }

    fn validate_auto_compaction_policy(&self) -> Result<()> {
        if !self.table.schema().primary_keys().is_empty() {
            let options = self.table.schema().core_options();
            options.compaction_force_up_level0()?;
            let lookup = options.try_changelog_producer()?
                == crate::spec::ChangelogProducer::Lookup
                || options.merge_engine()? == crate::spec::MergeEngine::FirstRow
                || options.deletion_vectors_enabled();
            if lookup {
                match self.table.schema().options().get("lookup-compact").map(|value| value.to_ascii_lowercase()).as_deref() {
                    None | Some("radical") => {}
                    Some("gentle") => return Err(crate::Error::Unsupported {
                        message: "Automatic compaction does not support lookup-compact=gentle trigger intervals; use write-only=true with dedicated compaction".into(),
                    }),
                    Some(value) => return Err(crate::Error::ConfigInvalid {
                        message: format!("Invalid lookup-compact: {value}"),
                    }),
                }
            }
        }
        Ok(())
    }

    pub(super) async fn commit_with_auto_compact(
        &self,
        plan: CommitEntriesPlan,
        expected_snapshot_id: Option<i64>,
        commit_identifier: i64,
        filter_committed: bool,
    ) -> Result<()> {
        let entries = match &plan {
            CommitEntriesPlan::Direct { entries, .. } => entries,
            CommitEntriesPlan::Overwrite { new_entries, .. } => new_entries,
            CommitEntriesPlan::Compact { .. } => unreachable!(),
        };
        let compact =
            self.auto_compaction_enabled(entries.iter().any(|e| *e.kind() == FileKind::Add))?;
        let kind = plan.commit_kind_hint();
        self.try_commit(
            plan,
            expected_snapshot_id,
            commit_identifier,
            filter_committed,
        )
        .await?;
        if compact {
            self.compact_after_write(commit_identifier, &kind).await?;
        }
        Ok(())
    }

    pub(super) async fn compact_after_write(
        &self,
        commit_identifier: i64,
        kind: &CommitKind,
    ) -> Result<()> {
        let result = async {
            let latest = self.snapshot_manager.get_latest_snapshot().await?;
            let earliest = self
                .snapshot_manager
                .earliest_snapshot_id()
                .await?
                .unwrap_or(1);
            let Some(written) = self
                .find_duplicate_commit(earliest, &latest, commit_identifier, kind)
                .await?
            else {
                // Recovery may filter an identifier older than retained snapshots.
                return Ok(());
            };
            let mut compact = self.clone();
            // Manifest names contain the data commit's random identity. A
            // digest keeps our recovery user stable without occupying a common,
            // otherwise legal application name such as `job-auto-compact`.
            let mut identity = Sha256::new();
            identity.update(self.table.location().as_bytes());
            identity.update(self.table.branch().as_bytes());
            identity.update(written.delta_manifest_list().as_bytes());
            compact.commit_user = format!(
                "{}-auto-compact-{}",
                self.commit_user,
                hex::encode(identity.finalize())
            );
            // Snapshot IDs also distinguish repeated batch commits using i64::MAX.
            if compact
                .is_committed_identifier_since(&latest, written.id(), Some(written.id() + 1))
                .await?
            {
                return Ok(());
            }
            // A historical writer can publish old-schema files against a newer
            // schema. Compaction must read and write the current field layout.
            let current_schema = if let Some(env) = &self.table.rest_env {
                let response = env.current_table_identity_checked().await?;
                let schema_id = response
                    .schema_id
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: "REST table response is missing schemaId".into(),
                        source: None,
                    })?;
                let schema = response.schema.ok_or_else(|| crate::Error::DataInvalid {
                    message: "REST table response is missing schema".into(),
                    source: None,
                })?;
                let schema = crate::spec::TableSchema::new(schema_id, &schema);
                self.table
                    .schema_manager()
                    .cache_resolved_schema(schema.clone());
                (schema_id != self.table.schema().id()).then_some(schema)
            } else {
                let schema_id = self.latest_schema_id().await?;
                if schema_id == self.table.schema().id() {
                    None
                } else {
                    Some(
                        self.table
                            .schema_manager()
                            .schema(schema_id)
                            .await?
                            .as_ref()
                            .clone(),
                    )
                }
            };
            let table = if let Some(schema) = current_schema {
                let overrides = self
                    .table
                    .applied_dynamic_option_keys
                    .iter()
                    .filter_map(|key| {
                        self.table
                            .schema()
                            .options()
                            .get(key)
                            .map(|value| (key.clone(), value.clone()))
                    })
                    .collect();
                self.table.copy_with_resolved_schema(
                    schema.copy_with_options(overrides),
                    self.table.branch(),
                )?
            } else {
                self.table.clone()
            };
            // Overwrite changelog suppression applies only to that write's
            // files. In recovery, a later APPEND may already have published
            // L0 records in this bucket whose changelog must remain pending.
            let overwrite = *kind == CommitKind::OVERWRITE;
            let snapshot = if overwrite {
                Some(&written)
            } else {
                latest.as_ref()
            };
            let table = table.copy_with_pinned_snapshot(snapshot);
            compact = Self::new(table, compact.commit_user);
            compact.snapshot_commit = self.snapshot_commit.clone();
            compact.snapshot_properties = self.snapshot_properties.clone();
            compact.commit_callbacks = self.commit_callbacks.clone();
            if self.table.rest_env.is_some() {
                compact
                    .table
                    .schema_manager()
                    .inherit_cached_schemas(self.table.schema_manager())?;
            }
            match crate::table::compact::ensure_supported(&compact.table) {
                Ok(()) => {}
                Err(crate::Error::Unsupported { .. }) => return Ok(()),
                Err(error) => return Err(error),
            }
            compact.validate_auto_compaction_policy()?;
            // Restore the exact writer ownership scope even when recovery was
            // given messages whose files have since been compacted or expired.
            let buckets = self
                .read_delta_entries(None, &written)
                .await?
                .into_iter()
                .filter(|entry| *entry.kind() == FileKind::Add)
                .map(|entry| {
                    (
                        if self.table.schema().partition_keys().is_empty() {
                            EMPTY_SERIALIZED_ROW.clone()
                        } else {
                            entry.partition().to_vec()
                        },
                        entry.bucket(),
                        entry.total_buckets(),
                    )
                })
                .collect::<crate::table::compact::CompactBuckets>();
            if buckets.is_empty() {
                return Ok(());
            }
            let builder = compact.table.new_compact_builder();
            let plan = match builder.new_planner()?.for_write(buckets).plan().await {
                Ok(plan) => plan,
                // Some limitations depend on snapshot metadata, for example
                // global indexes outside Data Evolution mode.
                Err(crate::Error::Unsupported { .. }) => return Ok(()),
                Err(error) => return Err(error),
            };
            if plan.tasks().is_empty() {
                return Ok(());
            }
            if overwrite
                && latest
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.id() != written.id())
            {
                let active = crate::table::snapshot_manifest::read_snapshot_entries(
                    &self.snapshot_manager,
                    latest.as_ref().expect("latest contains the data snapshot"),
                    None,
                )
                .await?;
                let active = active
                    .iter()
                    .map(|entry| {
                        (
                            (
                                if compact.table.schema().partition_keys().is_empty() {
                                    EMPTY_SERIALIZED_ROW.clone()
                                } else {
                                    entry.partition().to_vec()
                                },
                                entry.bucket(),
                                entry.total_buckets(),
                                entry.file().file_name.clone(),
                            ),
                            entry.file(),
                        )
                    })
                    .collect::<HashMap<_, _>>();
                if plan.tasks().iter().any(|task| {
                    task.files().iter().any(|file| {
                        active
                            .get(&(
                                task.partition().to_vec(),
                                task.bucket(),
                                task.total_buckets(),
                                file.file_name.clone(),
                            ))
                            .is_none_or(|current| *current != file)
                    })
                }) {
                    // Another rewrite already replaced these inputs. Replaying
                    // old overwrite work must not resurrect them or consume
                    // newer records while trying to suppress their changelog.
                    return Ok(());
                }
            }
            let executor = builder.new_auto_executor()?;
            let executor = if *kind == CommitKind::OVERWRITE {
                executor.without_changelog()
            } else {
                executor
            };
            let options = compact.table.schema().core_options();
            let plain_stateful = !compact.table.schema().primary_keys().is_empty()
                && matches!(
                    options.merge_engine()?,
                    crate::spec::MergeEngine::PartialUpdate | crate::spec::MergeEngine::Aggregation
                )
                && options.try_changelog_producer()? != crate::spec::ChangelogProducer::Lookup;
            let mut results = Vec::with_capacity(plan.tasks().len());
            let prepared = async {
                for task in plan.tasks() {
                    results.push(executor.execute(task).await?);
                }
                crate::table::compact::prepare_results(&compact.table, &results)
            }
            .await;
            let prepared = match prepared {
                Ok(prepared) => prepared,
                Err(error) => {
                    // No publication has been attempted. Only completed task
                    // outputs are ours to discard; the failed executor cleans
                    // its own partial files. Retain outputs after try_commit.
                    let messages = plan
                        .tasks()
                        .iter()
                        .zip(&results)
                        .map(|(task, result)| {
                            let mut message = CommitMessage::new(
                                task.partition().to_vec(),
                                task.bucket(),
                                result.files().to_vec(),
                            );
                            message.new_changelog_files = result.changelog_files().to_vec();
                            message
                        })
                        .collect::<Vec<_>>();
                    let _ = compact.abort(&messages).await;
                    // Some valid raw retract states have no lossless compact
                    // encoding yet. Keep those source files instead of making
                    // an ordinary write fail. Lookup errors still propagate:
                    // silently skipping those would lose required changelogs.
                    if plain_stateful && matches!(error, crate::Error::Unsupported { .. }) {
                        return Ok(());
                    }
                    return Err(error);
                }
            };
            compact
                .try_commit(
                    CommitEntriesPlan::Compact {
                        entries: prepared.entries,
                        changelog_entries: prepared.changelog_entries,
                        deletion_vector_updates: prepared.deletion_vector_updates,
                        schema_id: prepared.schema_id,
                        read_snapshot: prepared.read_snapshot,
                        planned_snapshot_id: prepared.snapshot_id,
                    },
                    None,
                    written.id(),
                    true,
                )
                .await
        }
        .await;
        result.map_err(|error| crate::Error::UnexpectedError {
            message: format!("Data commit {commit_identifier} succeeded, but automatic compaction failed; retry through the corresponding recovery API with the same commit user and identifier"),
            source: Some(Box::new(error)),
        })
    }
}
