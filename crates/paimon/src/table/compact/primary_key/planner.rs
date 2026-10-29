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

//! Java UniversalCompaction run selection. Apply normal size/run triggers
//! before forcing L0 progress, as ForceUpLevel0Compaction does, without a
//! full-bucket override.

use crate::spec::{ChangelogProducer, DataFileMeta, MergeEngine, TableSchema};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::table) struct PrimaryKeyTask {
    pub output_level: i32,
}

pub(in crate::table::compact) fn enabled(schema: &TableSchema) -> Result<bool> {
    let options = schema.core_options();
    Ok(options.merge_engine()? == MergeEngine::Deduplicate
        && !options.deletion_vectors_enabled()
        && matches!(
            options.try_changelog_producer()?,
            ChangelogProducer::None | ChangelogProducer::Input
        ))
}

fn number(
    options: &std::collections::HashMap<String, String>,
    key: &str,
    default: i32,
    minimum: i32,
) -> Result<i32> {
    let value = options
        .get(key)
        .map(|value| value.parse::<i32>())
        .transpose()
        .map_err(|_| Error::ConfigInvalid {
            message: format!("Invalid {key}"),
        })?
        .unwrap_or(default);
    if value < minimum {
        return Err(Error::ConfigInvalid {
            message: format!("{key} must be at least {minimum}"),
        });
    }
    Ok(value)
}

pub(in crate::table::compact) fn select(
    files: Vec<DataFileMeta>,
    properties: &std::collections::HashMap<String, String>,
) -> Result<Option<(Vec<DataFileMeta>, PrimaryKeyTask)>> {
    let trigger = number(properties, "num-sorted-run.compaction-trigger", 5, 1)?;
    let configured_levels = number(properties, "num-levels", trigger.saturating_add(1), 2)?;
    let levels = configured_levels.max(
        files
            .iter()
            .map(|file| file.level.saturating_add(1))
            .max()
            .unwrap_or(2),
    );
    let ratio = number(properties, "compaction.size-ratio", 1, 0)?;
    let amplification = number(
        properties,
        "compaction.max-size-amplification-percent",
        200,
        0,
    )?;
    let mut l0 = files
        .iter()
        .filter(|file| file.level == 0)
        .cloned()
        .collect::<Vec<_>>();
    l0.sort_by(|a, b| {
        b.max_sequence_number
            .cmp(&a.max_sequence_number)
            .then_with(|| a.min_sequence_number.cmp(&b.min_sequence_number))
            .then_with(|| {
                a.creation_time
                    .unwrap_or_default()
                    .cmp(&b.creation_time.unwrap_or_default())
            })
            .then_with(|| a.file_name.cmp(&b.file_name))
    });
    let mut positive = BTreeMap::<i32, Vec<DataFileMeta>>::new();
    for file in files.into_iter().filter(|file| file.level > 0) {
        positive.entry(file.level).or_default().push(file);
    }
    let l0_count = l0.len();
    let mut runs = l0
        .into_iter()
        .map(|file| (0, vec![file]))
        .collect::<Vec<_>>();
    for (level, mut files) in positive {
        files.sort_by(|a, b| a.file_name.cmp(&b.file_name));
        runs.push((level, files));
    }
    if runs.is_empty() {
        return Ok(None);
    }
    let sizes = runs
        .iter()
        .map(|(_, files)| {
            files
                .iter()
                .map(|file| file.file_size as u128)
                .sum::<u128>()
        })
        .collect::<Vec<_>>();
    let mut count = None;
    if runs.len() >= trigger as usize {
        let additional = sizes[..sizes.len() - 1].iter().sum::<u128>();
        if additional * 100 > amplification as u128 * sizes[sizes.len() - 1] {
            count = Some(runs.len());
        } else {
            count = pick_for_size_ratio(&runs, &sizes, ratio, 1, false);
        }
        if count.is_none() && runs.len() > trigger as usize {
            count = pick_for_size_ratio(
                &runs,
                &sizes,
                ratio,
                runs.len() - trigger as usize + 1,
                false,
            );
        }
    }
    if count.is_none() && l0_count > 0 {
        count = pick_for_size_ratio(&runs, &sizes, ratio, l0_count, true);
    }
    let Some(count) = count else {
        return Ok(None);
    };
    let output_level = if count == runs.len() {
        levels - 1
    } else {
        (runs[count].0 - 1).max(1)
    };
    let selected = runs[..count]
        .iter()
        .flat_map(|(_, files)| files.clone())
        .collect::<Vec<_>>();
    if selected.len() == 1 && selected[0].level == output_level {
        return Ok(None);
    }
    Ok(Some((selected, PrimaryKeyTask { output_level })))
}

fn pick_for_size_ratio(
    runs: &[(i32, Vec<DataFileMeta>)],
    sizes: &[u128],
    ratio: i32,
    mut count: usize,
    force: bool,
) -> Option<usize> {
    let mut size = sizes[..count].iter().sum::<u128>();
    let mut triggered = force || count > 1;
    while count < runs.len() {
        // Match Java's double arithmetic, including threshold rounding for
        // large runs, rather than comparing exact integer cross-products.
        if size as f64 * (100.0 + f64::from(ratio)) / 100.0 < sizes[count] as f64
            && (!triggered || runs[count].0 > 1)
        {
            break;
        }
        size += sizes[count];
        count += 1;
        triggered = true;
    }
    triggered.then_some(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use std::collections::HashMap;
    fn file(name: &str, level: i32, size: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.into(),
            file_size: size,
            row_count: 1,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 1,
            schema_id: 0,
            level,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: Some(0),
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        }
    }
    #[test]
    fn java_force_l0_stops_before_a_large_higher_level_and_outputs_below_it() {
        let (selected, task) = select(
            vec![file("new", 0, 1), file("anchor", 5, 1000)],
            &HashMap::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["new"]
        );
        assert_eq!(task.output_level, 4);
        let (selected, task) = select(
            vec![
                file("new", 0, 1),
                file("l1", 1, 100),
                file("anchor", 5, 10000),
            ],
            &HashMap::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            selected.len(),
            2,
            "Java always consumes L1 rather than outputting L0"
        );
        assert_eq!(task.output_level, 4);
    }
    #[test]
    fn positive_run_count_and_size_amplification_trigger_incremental_maintenance() {
        let options = HashMap::from([
            ("num-sorted-run.compaction-trigger".into(), "2".into()),
            ("num-levels".into(), "6".into()),
        ]);
        assert!(
            select(vec![file("small", 3, 1), file("anchor", 5, 1000)], &options)
                .unwrap()
                .is_none()
        );
        let (selected, task) = select(
            vec![
                file("small", 3, 1),
                file("medium", 4, 10),
                file("anchor", 5, 1000),
            ],
            &options,
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(task.output_level, 4);
        let (selected, task) = select(
            vec![
                file("large", 1, 1000),
                file("other", 3, 1000),
                file("old", 5, 100),
            ],
            &options,
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.len(), 3);
        assert_eq!(task.output_level, 5);
    }

    #[test]
    fn selection_is_stable_and_keeps_each_positive_run_together() {
        let mut older = file("older-l0", 0, 1);
        older.max_sequence_number = 10;
        let mut newer = file("newer-l0", 0, 1);
        newer.max_sequence_number = 20;
        let files = vec![
            file("anchor", 5, 100000),
            file("b", 1, 100),
            older,
            file("a", 1, 100),
            newer,
        ];
        let expected = select(files.clone(), &HashMap::new()).unwrap().unwrap();
        assert_eq!(
            expected
                .0
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["newer-l0", "older-l0", "a", "b"]
        );
        assert_eq!(expected.1.output_level, 4);
        let reversed = files.into_iter().rev().collect();
        assert_eq!(
            select(reversed, &HashMap::new()).unwrap().unwrap(),
            expected
        );
        let mut ties = vec![];
        for (name, minimum, time) in [("a-later", 2, 2), ("z-earlier", 2, 1), ("z-minimum", 1, 2)] {
            let mut row = file(name, 0, 1);
            row.min_sequence_number = minimum;
            row.max_sequence_number = 20;
            row.creation_time = chrono::DateTime::from_timestamp_millis(time);
            ties.push(row);
        }
        let selected = select(ties, &HashMap::new()).unwrap().unwrap();
        assert_eq!(
            selected
                .0
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["z-minimum", "z-earlier", "a-later"]
        );
    }

    #[test]
    fn size_ratio_and_amplification_use_java_threshold_boundaries() {
        let options = HashMap::from([("num-sorted-run.compaction-trigger".into(), "3".into())]);
        for (middle, all) in [(199, false), (200, true)] {
            let selected = select(
                vec![
                    file("small", 2, 1),
                    file("middle", 3, middle),
                    file("old", 5, 100),
                ],
                &options,
            )
            .unwrap();
            if all {
                assert_eq!(selected.unwrap().0.len(), 3);
            } else {
                assert!(selected.is_none());
            }
        }
        for (first, merge) in [(199, false), (200, true)] {
            let selected = select(
                vec![
                    file("first", 2, first),
                    file("second", 3, 202),
                    file("old", 5, 100000),
                ],
                &options,
            )
            .unwrap();
            if merge {
                let (files, task) = selected.unwrap();
                assert_eq!(files.len(), 2);
                assert_eq!(task.output_level, 4);
            } else {
                assert!(selected.is_none());
            }
        }
        // Arithmetic must not overflow when byte sizes exceed i64 percentages.
        let (files, task) = select(
            vec![file("first", 0, i64::MAX), file("old", 5, i64::MAX)],
            &HashMap::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(task.output_level, 5);
    }

    #[test]
    fn singletons_empty_inputs_and_existing_levels_do_not_force_a_full_rewrite() {
        assert!(select(vec![], &HashMap::new()).unwrap().is_none());
        assert!(select(vec![file("old", 5, 100)], &HashMap::new())
            .unwrap()
            .is_none());
        let options = HashMap::from([("num-levels".into(), "3".into())]);
        let (_, task) = select(vec![file("new", 0, 1)], &options).unwrap().unwrap();
        assert_eq!(task.output_level, 2);
        let (_, task) = select(vec![file("new", 0, 100), file("old", 8, 1)], &options)
            .unwrap()
            .unwrap();
        assert_eq!(task.output_level, 8, "existing higher levels remain valid");
    }

    #[test]
    fn invalid_run_policy_options_are_reported_before_selection() {
        for (name, bad, minimum) in [
            ("num-sorted-run.compaction-trigger", "0", "1"),
            ("num-levels", "1", "2"),
            ("compaction.size-ratio", "-1", "0"),
            ("compaction.max-size-amplification-percent", "-1", "0"),
        ] {
            for value in [bad, "invalid", "2147483648"] {
                let options = HashMap::from([(name.into(), value.into())]);
                let error = select(vec![file("new", 0, 1)], &options).unwrap_err();
                assert!(matches!(error, Error::ConfigInvalid { .. }));
                assert!(error.to_string().contains(name));
            }
            let options = HashMap::from([(name.into(), minimum.into())]);
            assert!(select(vec![file("new", 0, 1)], &options).unwrap().is_some());
        }
    }

    #[test]
    fn normal_maintenance_takes_precedence_over_forcing_l0() {
        let (files, task) = select(
            vec![
                file("new", 0, 1),
                file("l2", 2, 1000),
                file("l3", 3, 1000),
                file("l4", 4, 1000),
                file("old", 5, 100),
            ],
            &HashMap::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            files.len(),
            5,
            "space amplification must run even when L0 exists"
        );
        assert_eq!(task.output_level, 5);

        let options = HashMap::from([("num-sorted-run.compaction-trigger".into(), "2".into())]);
        let (files, task) = select(
            vec![
                file("new", 0, 1),
                file("small", 3, 100),
                file("old", 5, 100000),
            ],
            &options,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            files.len(),
            2,
            "run-count maintenance must precede forced L0 progress"
        );
        assert_eq!(task.output_level, 4);
    }

    #[test]
    fn large_run_size_ratio_matches_java_double_threshold() {
        let options = HashMap::from([("num-sorted-run.compaction-trigger".into(), "2".into())]);
        let (files, task) = select(
            vec![
                file("newer", 2, 100_000_000_000_099),
                file("older", 5, 101_000_000_000_100),
            ],
            &options,
        )
        .unwrap()
        .expect("Java rounds the 1% threshold up to the older run's size");
        assert_eq!(files.len(), 2);
        assert_eq!(task.output_level, 5);
    }
}
