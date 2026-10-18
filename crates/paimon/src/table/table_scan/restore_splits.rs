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

//! Restore transported plans only after proving their files and logical groups.

use super::*;

fn same_path(a: &str, b: &str) -> bool {
    fn local(path: &str) -> Option<&str> {
        path.strip_prefix("file:/")
            .or_else(|| path.starts_with('/').then_some(path))
            .map(|path| path.trim_start_matches('/'))
    }
    a == b || matches!((local(a),local(b)), (Some(a),Some(b)) if a == b)
}

pub(super) async fn validate(
    scan: &TableScan<'_>,
    splits: &[DataSplit],
    grant: &Arc<super::super::query_auth::QueryAuthGrant>,
) -> crate::Result<Vec<DataSplit>> {
    let TableScanKind::Paimon(base) = &scan.0 else {
        return Err(super::super::query_auth::unsupported(
            "only Paimon data splits carry query authorization",
        ));
    };
    let mut plans = HashMap::new();
    let mut normalized = Vec::with_capacity(splits.len());
    if let Some(view) = &base.table.travel_snapshot {
        if splits.iter().any(|split| match view {
            super::super::ReadSnapshot::Empty => !split.data_files().is_empty(),
            super::super::ReadSnapshot::Snapshot(snapshot) => snapshot.id() != split.snapshot_id(),
        }) {
            return Err(super::super::query_auth::unsupported(
                "transported splits address another captured snapshot",
            ));
        }
    }
    for split in splits {
        let key = (split.snapshot_id(), split.is_streaming());
        if let std::collections::hash_map::Entry::Vacant(entry) = plans.entry(key) {
            let snapshot = match &base.table.travel_snapshot {
                Some(super::super::ReadSnapshot::Snapshot(snapshot))
                    if snapshot.id() == split.snapshot_id() =>
                {
                    snapshot.as_ref().clone()
                }
                _ => {
                    base.table
                        .snapshot_manager()
                        .get_snapshot(split.snapshot_id())
                        .await?
                }
            };
            let mut scan = base.restricted_by(grant);
            scan.table = ScanTable::Owned(Arc::new(
                base.table
                    .copy_with_pinned_snapshot(Some(&snapshot))
                    .copy_with_options(HashMap::from([(
                        "source.split.target-size".into(),
                        "1b".into(),
                    )])),
            ));
            scan.scan_all_files = splits.iter().any(|s| {
                s.snapshot_id() == split.snapshot_id()
                    && s.data_files().iter().any(|file| file.level == 0)
            });
            let projected = scan.clone();
            scan.projected_read_field_ids = None;
            let plan = if split.is_streaming() {
                let mut stats = ScanStats::default();
                let delta = scan.plan_snapshot_delta(&snapshot, &mut stats).await?;
                let changelog = scan.plan_snapshot_changelog(&snapshot, &mut stats).await?;
                let files = delta
                    .splits()
                    .iter()
                    .chain(changelog.splits())
                    .cloned()
                    .collect();
                scan.finish_plan(Plan::new(files)).await?
            } else {
                scan.plan().await?
            };
            let required = if base.table.schema().core_options().data_evolution_enabled()
                && !split.is_streaming()
            {
                Some(projected.plan().await?)
            } else {
                None
            };
            entry.insert((plan, required));
        }
        let (plan, required) = &plans[&key];
        let candidates: Vec<_> = plan
            .splits()
            .iter()
            .filter(|candidate| {
                candidate.partition() == split.partition()
                    && candidate.bucket() == split.bucket()
                    && same_path(candidate.bucket_path(), split.bucket_path())
                    && split
                        .declared_total_buckets()
                        .is_none_or(|total| total == candidate.total_buckets())
            })
            .collect();
        let mut seen = HashSet::new();
        let mut canonical = split.clone();
        for (index, file) in split.data_files().iter().enumerate() {
            if !seen.insert(&file.file_name) {
                return Err(super::super::query_auth::unsupported(
                    "transported split repeats a physical data file",
                ));
            }
            let source = candidates.iter().find_map(|candidate| {
                candidate
                    .data_files()
                    .iter()
                    .position(|known| known == file)
                    .map(|position| (*candidate, position))
            });
            let Some((source, position)) = source else {
                return Err(super::super::query_auth::unsupported(
                    "transported file is not part of this table's snapshot",
                ));
            };
            let deletion = |split: &DataSplit, index: usize| {
                split
                    .data_deletion_files()
                    .and_then(|files| files.get(index))
                    .and_then(Option::as_ref)
                    .cloned()
            };
            if deletion(split, index) != deletion(source, position) {
                return Err(super::super::query_auth::unsupported(
                    "transported deletion vector differs from the snapshot",
                ));
            }
            canonical = canonical.restore_bucket_layout(source);
            if let Some(required) = required {
                let needed: Vec<_> = source
                    .data_files()
                    .iter()
                    .filter(|file| {
                        required.splits().iter().any(|candidate| {
                            candidate.bucket() == split.bucket()
                                && candidate.partition() == split.partition()
                                && candidate.data_files().contains(file)
                        })
                    })
                    .collect();
                let supplied = source
                    .data_files()
                    .iter()
                    .filter(|file| split.data_files().contains(file))
                    .count();
                if needed.iter().any(|file| !split.data_files().contains(file))
                    || (!source.raw_convertible() && supplied > 1 && split.raw_convertible())
                {
                    return Err(super::super::query_auth::unsupported(
                        "transported split omits required column files or their row merging",
                    ));
                }
            } else if !source.raw_convertible()
                && (split.raw_convertible()
                    || source
                        .data_files()
                        .iter()
                        .any(|needed| !split.data_files().contains(needed)))
            {
                return Err(super::super::query_auth::unsupported(
                    "transported split omits files required for logical row merging",
                ));
            }
        }
        normalized.push(canonical);
    }
    Ok(normalized)
}
