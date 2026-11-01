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

//! Shared Java UniversalCompaction prefix selection for data and index runs.
//!
//! Callers supply ordered runs and their own cost unit: PK uses data bytes,
//! global indexes use logical rows. Source layout and output levels stay local.

use crate::{Error, Result};
use std::collections::HashMap;

pub(super) struct Run {
    pub size: u128,
    /// PK L0/L1 must be consumed once a merge starts; other boundaries may stop
    /// before a larger run. Global index runs allow stopping at every boundary.
    pub stop_before_large: bool,
}

pub(super) struct UniversalCompaction {
    pub trigger: i32,
    ratio: i32,
    amplification: i32,
}

impl UniversalCompaction {
    pub fn from_options(options: &HashMap<String, String>, prefix: &str) -> Result<Self> {
        Ok(Self {
            trigger: integer_option(
                options,
                &format!("{prefix}num-sorted-run.compaction-trigger"),
                5,
                1,
            )?,
            ratio: integer_option(options, &format!("{prefix}compaction.size-ratio"), 1, 0)?,
            amplification: integer_option(
                options,
                &format!("{prefix}compaction.max-size-amplification-percent"),
                200,
                0,
            )?,
        })
    }

    pub fn pick(&self, runs: &[Run], forced_minimum: usize) -> Option<usize> {
        if runs.is_empty() {
            return None;
        }
        let mut count = None;
        if runs.len() >= self.trigger as usize {
            let additional = runs[..runs.len() - 1]
                .iter()
                .map(|run| run.size)
                .sum::<u128>();
            if additional * 100 > self.amplification as u128 * runs[runs.len() - 1].size {
                count = Some(runs.len());
            } else {
                count = self.pick_for_size_ratio(runs, 1, false);
            }
            if count.is_none() && runs.len() > self.trigger as usize {
                count =
                    self.pick_for_size_ratio(runs, runs.len() - self.trigger as usize + 1, false);
            }
        }
        if count.is_none() && forced_minimum > 0 {
            count = self.pick_for_size_ratio(runs, forced_minimum, true);
        }
        count
    }

    fn pick_for_size_ratio(&self, runs: &[Run], mut count: usize, force: bool) -> Option<usize> {
        let mut size = runs[..count].iter().map(|run| run.size).sum::<u128>();
        let mut triggered = force || count > 1;
        while count < runs.len() {
            // Preserve Java's double arithmetic, including rounding for large runs.
            if size as f64 * (100.0 + f64::from(self.ratio)) / 100.0 < runs[count].size as f64
                && (!triggered || runs[count].stop_before_large)
            {
                break;
            }
            size += runs[count].size;
            count += 1;
            triggered = true;
        }
        triggered.then_some(count)
    }
}

pub(super) fn integer_option(
    options: &HashMap<String, String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn runs(sizes: &[u128]) -> Vec<Run> {
        sizes
            .iter()
            .map(|&size| Run {
                size,
                stop_before_large: true,
            })
            .collect()
    }

    #[test]
    fn global_prefix_is_independent_of_data_compaction_and_stops_before_a_large_base() {
        let policy = UniversalCompaction::from_options(
            &HashMap::from([
                ("num-sorted-run.compaction-trigger".into(), "1".into()),
                (
                    "global-index.num-sorted-run.compaction-trigger".into(),
                    "3".into(),
                ),
            ]),
            "global-index.",
        )
        .unwrap();
        assert!(policy.pick(&runs(&[1, 1000]), 0).is_none());
        assert_eq!(policy.pick(&runs(&[1, 1, 1000]), 0), Some(2));
        assert_eq!(policy.pick(&runs(&[1, 10, 100, 1000]), 0), Some(2));
        assert_eq!(policy.pick(&runs(&[1000, 1000, 100]), 0), Some(3));
    }

    #[test]
    fn pk_force_consumes_l1_but_does_not_force_global_index_merges() {
        let policy = UniversalCompaction::from_options(&HashMap::new(), "").unwrap();
        let runs = vec![
            Run {
                size: 1,
                stop_before_large: false,
            },
            Run {
                size: 100,
                stop_before_large: false,
            },
            Run {
                size: 10000,
                stop_before_large: true,
            },
        ];
        assert!(policy.pick(&runs, 0).is_none());
        assert_eq!(policy.pick(&runs, 1), Some(2));
        assert_eq!(
            policy.pick(
                &[Run {
                    size: 1,
                    stop_before_large: false
                }],
                1
            ),
            Some(1)
        );
    }
}
