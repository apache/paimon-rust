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

use crate::spec::{CoreOptions, DataFileMeta};
use crate::Result;

/// Batch packing from Java AppendCompactCoordinator.SubCoordinator.pack.
/// Compaction uses file size + open cost; read splits use max(size, open cost).
pub(super) struct AppendCompactPlanner {
    small_file_size: i64,
    target_weight: i128,
    open_file_cost: i64,
    min_file_num: usize,
}

impl AppendCompactPlanner {
    pub(super) fn new(options: &CoreOptions<'_>) -> Result<Self> {
        Ok(Self {
            small_file_size: options.compaction_file_size()?,
            target_weight: i128::from(options.target_file_size()) * 2,
            open_file_cost: options.source_split_open_file_cost(),
            min_file_num: options.compaction_min_file_num()?,
        })
    }

    pub(super) fn plan(&self, mut files: Vec<DataFileMeta>) -> Vec<Vec<DataFileMeta>> {
        files.retain(|file| file.file_size < self.small_file_size);
        // Size ordering matches Java; sequence/name break ties deterministically
        // so regrouping input read splits cannot change task boundaries.
        files.sort_by(|a, b| {
            (a.file_size, a.min_sequence_number, &a.file_name).cmp(&(
                b.file_size,
                b.min_sequence_number,
                &b.file_name,
            ))
        });
        let mut tasks = Vec::new();
        let mut pending = Vec::new();
        let mut weight = 0i128;
        for file in files {
            weight += i128::from(file.file_size) + i128::from(self.open_file_cost);
            pending.push(file);
            if pending.len() > 1 && weight >= self.target_weight {
                tasks.push(std::mem::take(&mut pending));
                weight = 0;
            }
        }
        if pending.len() >= self.min_file_num {
            tasks.push(pending);
        }
        tasks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use std::collections::HashMap;

    fn files(sizes: &[i64]) -> Vec<DataFileMeta> {
        sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| DataFileMeta {
                file_name: format!("file-{i}.parquet"),
                file_size: size,
                row_count: 1,
                min_key: vec![],
                max_key: vec![],
                key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
                value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
                min_sequence_number: i as i64,
                max_sequence_number: i as i64,
                schema_id: 0,
                level: 0,
                extra_files: vec![],
                creation_time: None,
                delete_row_count: None,
                embedded_index: None,
                first_row_id: None,
                write_cols: None,
                external_path: None,
                file_source: None,
                value_stats_cols: None,
                column_max_sequence_numbers: None,
            })
            .collect()
    }

    fn make_planner(options: &[(&str, &str)]) -> AppendCompactPlanner {
        let options = options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        AppendCompactPlanner::new(&CoreOptions::new(&options)).unwrap()
    }

    #[test]
    fn java_coordinator_packing_examples() {
        let options = HashMap::from([("compaction.min.file-num".into(), "3".into())]);
        let core = CoreOptions::new(&options);
        let target = core.target_file_size();
        let planner = AppendCompactPlanner::new(&core).unwrap();
        for (count, size, tasks) in [
            (200, 0, 2),
            (100, i64::MAX, 0),
            (100, target / 3 + 1, 17),
            (100, target / 10 * 8, 0),
            (1000, target / 10, 56),
            (1050, target / 5, 105),
            (1089, target / 5, 109),
        ] {
            let groups = planner.plan(files(&vec![size; count]));
            assert_eq!(groups.len(), tasks, "count={count}, size={size}");
        }
    }

    #[test]
    fn strict_small_file_ratio_threshold_and_minimum_tail() {
        let planner = make_planner(&[
            ("target-file-size", "100b"),
            ("source.split.open-file-cost", "0b"),
        ]);
        assert!(planner.plan(files(&[10; 4])).is_empty());
        let groups = planner.plan(files(&[10, 10, 10, 10, 10, 70, 80, 100]));
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 5);
        assert!(groups[0].iter().all(|file| file.file_size == 10));

        let custom = make_planner(&[
            ("target-file-size", "100b"),
            ("source.split.open-file-cost", "0b"),
            ("compaction.small-file-ratio", "0.5"),
            ("compaction.min.file-num", "2"),
        ]);
        assert_eq!(custom.plan(files(&[49, 49, 50, 70]))[0].len(), 2);
        assert!(make_planner(&[("compaction.small-file-ratio", "0")])
            .plan(files(&[0; 5]))
            .is_empty());
    }

    #[test]
    fn weight_adds_open_cost_and_emits_at_twice_target_with_at_least_two_files() {
        let planner = make_planner(&[
            ("target-file-size", "100b"),
            ("source.split.open-file-cost", "60b"),
        ]);
        let groups = planner.plan(files(&[40; 5]));
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 2]);
        assert!(make_planner(&[
            ("target-file-size", "100b"),
            ("source.split.open-file-cost", "59b"),
        ])
        .plan(files(&[40; 2]))
        .is_empty());
        assert!(make_planner(&[
            ("target-file-size", "100b"),
            ("source.split.open-file-cost", "200b"),
        ])
        .plan(files(&[40]))
        .is_empty());
    }

    #[test]
    fn compaction_sorts_by_size_while_read_splits_sort_by_sequence() {
        let input = files(&[60, 10, 90, 40, 70, 30]);
        let groups = make_planner(&[
            ("target-file-size", "100b"),
            ("compaction.small-file-ratio", "1"),
            ("source.split.open-file-cost", "0b"),
        ])
        .plan(input.clone());
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0]
                .iter()
                .map(|file| file.file_size)
                .collect::<Vec<_>>(),
            vec![10, 30, 40, 60, 70]
        );
        let read_splits = crate::table::bin_pack::split_for_batch(input, 100, 0);
        assert_eq!(
            read_splits
                .iter()
                .map(|split| split.iter().map(|file| file.file_size).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![60, 10], vec![90], vec![40], vec![70, 30]]
        );
    }

    #[test]
    fn task_weight_does_not_overflow_at_large_target_sizes() {
        let options = HashMap::from([
            ("target-file-size".into(), i64::MAX.to_string()),
            ("compaction.small-file-ratio".into(), "1".into()),
            ("source.split.open-file-cost".into(), "0b".into()),
        ]);
        let planner = AppendCompactPlanner::new(&CoreOptions::new(&options)).unwrap();
        assert!(planner.plan(files(&[i64::MAX / 2; 3])).is_empty());
        let groups = planner.plan(files(&[i64::MAX / 2; 5]));
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 5);
    }
}
