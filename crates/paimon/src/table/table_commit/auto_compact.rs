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
        // Reject invalid compaction configuration before publishing data.
        let builder = self.table.new_compact_builder();
        builder.new_planner()?;
        builder.new_executor()?;
        Ok(true)
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
            compact.commit_user = format!("{}-auto-compact", self.commit_user);
            // Snapshot IDs also distinguish repeated batch commits using i64::MAX.
            if compact
                .is_committed_identifier_since(&latest, written.id(), Some(written.id() + 1))
                .await?
            {
                return Ok(());
            }
            // A historical writer can publish old-schema files against a newer
            // schema. Compaction must read and write the current field layout.
            let schema_id = self.latest_schema_id().await?;
            let table = if schema_id == self.table.schema().id() {
                self.table.clone()
            } else {
                let schema = self.table.schema_manager().schema(schema_id).await?;
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
            };
            // Capture latest on the same branch, clearing historical read selectors.
            let table = table.copy_with_pinned_snapshot(latest.as_ref());
            compact = Self::new(table, compact.commit_user);
            compact.snapshot_commit = self.snapshot_commit.clone();
            compact.snapshot_properties = self.snapshot_properties.clone();
            compact.commit_callbacks = self.commit_callbacks.clone();
            match crate::table::compact::ensure_supported(&compact.table) {
                Ok(()) => {}
                Err(crate::Error::Unsupported { .. }) => return Ok(()),
                Err(error) => return Err(error),
            }
            let builder = compact.table.new_compact_builder();
            let plan = match builder.new_planner()?.plan().await {
                Ok(plan) => plan,
                // Some limitations depend on snapshot metadata, for example
                // global indexes outside Data Evolution mode.
                Err(crate::Error::Unsupported { .. }) => return Ok(()),
                Err(error) => return Err(error),
            };
            if plan.tasks().is_empty() {
                return Ok(());
            }
            let executor = builder.new_executor()?;
            let mut results = Vec::with_capacity(plan.tasks().len());
            for task in plan.tasks() {
                results.push(executor.execute(task).await?);
            }
            let prepared = crate::table::compact::prepare_results(&compact.table, &results)?;
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
