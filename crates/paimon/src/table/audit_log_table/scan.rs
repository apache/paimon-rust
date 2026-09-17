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

//! Audit scan wrapper: retain row versions and only forward safe pruning.

use super::{TableScan, TableScanKind};
use crate::spec::MergeEngine;
use crate::table::kv_file_reader::retain_primary_key_conjuncts;
use crate::table::merge_tree_split_generator::{merge_tree_split_for_batch, KeyComparator};
use crate::table::{DataSplit, Plan, ReadBuilder, ScanTrace};
use std::collections::HashMap;

/// Plans current-state audit reads over the wrapped table's files.
#[derive(Debug, Clone)]
pub struct AuditLogScan<'a> {
    scan: TableScan<'a>,
}

impl<'a> AuditLogScan<'a> {
    fn new(mut scan: TableScan<'a>) -> Self {
        if let TableScanKind::Paimon(inner) = &mut scan.0 {
            // Preserve the read projection, including data-evolution column pruning.
            inner.scan_all_files = true;
            inner.limit = None;
            inner.row_range_optimization_disabled = true;
            inner.row_ranges = None;
            if !inner.table.schema().primary_keys().is_empty() {
                inner.data_predicates = retain_primary_key_conjuncts(
                    &inner.data_predicates,
                    inner.table.schema().fields(),
                    &inner.table.schema().trimmed_primary_keys(),
                );
            }
        }
        Self { scan }
    }

    pub async fn plan(&self) -> crate::Result<Plan> {
        self.plan_with_trace().await.map(|(plan, _)| plan)
    }

    pub async fn plan_with_trace(&self) -> crate::Result<(Plan, ScanTrace)> {
        let TableScanKind::Paimon(inner) = &self.scan.0 else {
            return Err(crate::Error::Unsupported {
                message: "Format tables do not support audit log batch scan".to_string(),
            });
        };
        let (plan, mut trace) = self.scan.plan_with_trace().await?;
        let options = inner.table.schema().core_options();
        let engine = options.merge_engine()?;
        // Ordinary first-row and DV scans pack files without a key merge. Audit
        // reads need the existing merge-tree planner to retain overlapping versions.
        let plan = if engine == MergeEngine::FirstRow
            || (options.deletion_vectors_enabled() && !options.deletion_vectors_merge_on_read())
        {
            if let Some(comparator) = KeyComparator::from_table_schema(inner.table.schema()) {
                let mut buckets: HashMap<(Vec<u8>, i32), Vec<DataSplit>> = HashMap::new();
                for split in plan.into_splits() {
                    buckets
                        .entry((split.partition().to_serialized_bytes(), split.bucket()))
                        .or_default()
                        .push(split);
                }
                let mut splits = Vec::new();
                for bucket in buckets.into_values() {
                    let first = &bucket[0];
                    let files = bucket
                        .iter()
                        .flat_map(|split| split.data_files().iter().cloned())
                        .collect();
                    let deletion_files: HashMap<_, _> = bucket
                        .iter()
                        .flat_map(|split| {
                            split.data_files().iter().filter_map(move |file| {
                                split
                                    .deletion_file_for_data_file(file)
                                    .map(|deletion| (file.file_name.clone(), deletion.clone()))
                            })
                        })
                        .collect();
                    for group in merge_tree_split_for_batch(
                        files,
                        &comparator,
                        options.source_split_target_size(),
                        options.source_split_open_file_cost(),
                        matches!(engine, MergeEngine::Deduplicate | MergeEngine::FirstRow),
                    ) {
                        let mut builder = DataSplit::builder()
                            .with_snapshot(first.snapshot_id())
                            .with_partition(first.partition().clone())
                            .with_bucket(first.bucket())
                            .with_bucket_path(first.bucket_path().to_string())
                            .with_total_buckets(first.total_buckets())
                            .with_raw_convertible(group.raw_convertible);
                        if !deletion_files.is_empty() {
                            builder = builder.with_data_deletion_files(
                                group
                                    .files
                                    .iter()
                                    .map(|file| deletion_files.get(&file.file_name).cloned())
                                    .collect(),
                            );
                        }
                        splits.push(builder.with_data_files(group.files).build()?);
                    }
                }
                Plan::new(splits)
            } else {
                plan
            }
        } else {
            plan
        };
        trace.record_final_plan(
            plan.splits().len(),
            plan.splits().len(),
            plan.splits()
                .iter()
                .map(|split| split.data_files().len())
                .sum(),
        );
        trace.planned_data_file_bytes = plan.planned_data_file_bytes();
        Ok((plan, trace))
    }
}

impl<'a> ReadBuilder<'a> {
    /// Create an audit scan that retains every visible row version.
    pub fn new_audit_scan(&self) -> AuditLogScan<'a> {
        AuditLogScan::new(self.new_scan())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BinaryRowBuilder, CoreOptions, DataField, DataType, Datum, IntType, PredicateBuilder,
    };
    use crate::table::table_scan::{
        tests::{
            data_evolution_test_table, pk_stats_file, pk_stats_gate_table, setup_scan_trace_dirs,
            two_column_schema,
        },
        PaimonTableScan,
    };
    use crate::table::{CommitMessage, TableCommit};
    use std::collections::HashSet;
    #[test]
    fn test_audit_scan_all_files_preserves_data_evolution_projection() {
        let table = data_evolution_test_table(
            "memory:/de_audit_scan_projection",
            two_column_schema(0, "id", "name"),
        )
        .copy_with_options(HashMap::from([(
            "global-index.enabled".to_string(),
            "true".to_string(),
        )]));
        let projected = HashSet::from([1]);
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(1))
            .unwrap();
        let scan = PaimonTableScan::new(&table, None, vec![predicate], None, None, None)
            .with_projected_read_field_ids(Some(projected.clone()));
        let wrapped = AuditLogScan::new(TableScan(TableScanKind::Paimon(scan)));
        let TableScanKind::Paimon(scan) = wrapped.scan.0 else {
            panic!("expected Paimon scan")
        };

        assert!(scan.scan_all_files);
        assert_eq!(scan.projected_read_field_ids, Some(projected));
        assert!(
            scan.global_index_scan_settings(&CoreOptions::new(table.schema().options()), true,)
                .unwrap()
                .is_none(),
            "audit scans must not prune physical row versions via global indexes"
        );
    }

    #[tokio::test]
    async fn test_audit_stats_pruning_keeps_overlapping_versions() {
        for option in ["deletion-vectors.enabled", "merge-engine"] {
            let table_path = format!("memory:/audit_stats_{option}");
            let value = if option == "merge-engine" {
                "first-row"
            } else {
                "true"
            };
            let table = pk_stats_gate_table(&table_path).copy_with_options(HashMap::from([
                (option.to_string(), value.to_string()),
                ("source.split.target-size".to_string(), "1b".to_string()),
            ]));
            setup_scan_trace_dirs(&table).await;

            let mut old = pk_stats_file("old-version.parquet", (1, 5), (100, 200));
            old.level = 1;
            let mut new = pk_stats_file("new-version.parquet", (1, 5), (10, 60));
            // Compacted files on the same level have disjoint key ranges.
            new.level = 2;
            TableCommit::new(table.clone(), "dv-audit-gate-test".to_string())
                .commit(vec![CommitMessage::new(
                    BinaryRowBuilder::new(0).build_serialized(),
                    0,
                    vec![old, new],
                )])
                .await
                .unwrap();

            let fields = vec![
                DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
                DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
            ];
            let value_filter = PredicateBuilder::new(&fields)
                .greater_than("value", Datum::Int(90))
                .unwrap();
            let mut reader = table.new_read_builder();
            reader.with_filter(value_filter);

            let (ordinary_plan, ordinary_trace) =
                reader.new_scan().plan_with_trace().await.unwrap();
            assert!(ordinary_trace.manifest_entries_pruned_by_data_stats >= 1);
            assert_eq!(
                ordinary_plan
                    .splits()
                    .iter()
                    .map(|split| split.data_files().len())
                    .sum::<usize>(),
                1
            );

            let (audit_plan, audit_trace) =
                reader.new_audit_scan().plan_with_trace().await.unwrap();
            assert_eq!(audit_trace.manifest_entries_pruned_by_data_stats, 0);
            assert_eq!(
                audit_plan
                    .splits()
                    .iter()
                    .map(|split| split.data_files().len())
                    .sum::<usize>(),
                2,
                "both key versions must reach the audit merge path"
            );
            assert_eq!(
                audit_plan.splits().len(),
                1,
                "overlapping versions must be planned together"
            );
            assert!(!audit_plan.splits()[0].raw_convertible());
        }
    }
}
