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

//! Java UniversalCompaction run selection. Explicit compact drains L0 using
//! forcePickL0; positive-level maintenance uses size/run triggers, never a full
//! bucket override.

use crate::spec::DataFileMeta;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::table) struct PrimaryKeyTask {
    pub output_level: i32,
    pub lookup_files: Vec<DataFileMeta>,
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
    let trigger = number(properties, "num-sorted-run.compaction-trigger", 5, 2)?;
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
    let mut count;
    if l0_count > 0 {
        // Java prepare-commit forces L0 progress for lookup; the explicit Rust
        // compact API requests the same bounded incremental work for all modes.
        count = l0_count;
    } else {
        if runs.len() < trigger as usize {
            return Ok(None);
        }
        let additional = sizes[..sizes.len() - 1].iter().sum::<u128>();
        if additional * 100 > amplification as u128 * sizes[sizes.len() - 1] {
            count = runs.len();
        } else {
            count = 1;
        }
    }
    let mut size = sizes[..count].iter().sum::<u128>();
    while count < runs.len() {
        let next = &runs[count];
        if size * (100 + ratio as u128) < sizes[count] * 100
            && (count <= 1 && l0_count == 0 || next.0 > 1)
        {
            break;
        }
        size += sizes[count];
        count += 1;
    }
    if l0_count == 0 && count == 1 {
        if runs.len() <= trigger as usize {
            return Ok(None);
        }
        count = runs.len() - trigger as usize + 1;
        size = sizes[..count].iter().sum();
        while count < runs.len()
            && (size * (100 + ratio as u128) >= sizes[count] * 100 || runs[count].0 <= 1)
        {
            size += sizes[count];
            count += 1;
        }
    }
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
    let lookup_files = runs[count..]
        .iter()
        .flat_map(|(_, files)| files.clone())
        .collect();
    Ok(Some((
        selected,
        PrimaryKeyTask {
            output_level,
            lookup_files,
        },
    )))
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
        assert_eq!(task.lookup_files[0].file_name, "anchor");
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
        assert_eq!(task.lookup_files.len(), 1);
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
        assert!(task.lookup_files.is_empty());
    }
}
