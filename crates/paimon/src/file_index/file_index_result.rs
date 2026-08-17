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

use roaring::RoaringBitmap;

/// Result of evaluating a predicate against file indexes.
///
/// Every result is a conservative candidate set and must contain every matching
/// row. `Remain` represents the full candidate set, `Skip` represents no rows,
/// and `Selection` represents the rows which may match the predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FileIndexResult {
    /// The index cannot narrow the full candidate set.
    Remain,
    /// The file cannot contain matching rows.
    Skip,
    /// Only the listed zero-based row positions may match.
    Selection(RoaringBitmap),
}

impl FileIndexResult {
    /// Returns whether the file index result contains any possible matches.
    pub(crate) fn remain(&self) -> bool {
        match self {
            Self::Remain => true,
            Self::Skip => false,
            Self::Selection(selection) => !selection.is_empty(),
        }
    }

    /// Combines two file index results with logical AND.
    pub(crate) fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Skip, _) | (_, Self::Skip) => Self::Skip,
            (Self::Remain, other) | (other, Self::Remain) => other,
            (Self::Selection(mut left), Self::Selection(right)) => {
                left &= right;
                Self::Selection(left)
            }
        }
    }

    /// Combines two file index results with logical OR.
    pub(crate) fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::Remain, _) | (_, Self::Remain) => Self::Remain,
            (Self::Skip, other) | (other, Self::Skip) => other,
            (Self::Selection(mut left), Self::Selection(right)) => {
                left |= right;
                Self::Selection(left)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(rows: impl IntoIterator<Item = u32>) -> FileIndexResult {
        FileIndexResult::Selection(rows.into_iter().collect())
    }

    #[test]
    fn test_remain() {
        assert!(FileIndexResult::Remain.remain());
        assert!(!FileIndexResult::Skip.remain());
        assert!(selection([1]).remain());
        assert!(!selection([]).remain());
    }

    #[test]
    fn test_and() {
        let rows = selection([1, 2]);

        assert_eq!(
            FileIndexResult::Remain.and(FileIndexResult::Remain),
            FileIndexResult::Remain
        );
        assert_eq!(
            FileIndexResult::Remain.and(FileIndexResult::Skip),
            FileIndexResult::Skip
        );
        assert_eq!(FileIndexResult::Remain.and(rows.clone()), rows);
        assert_eq!(rows.clone().and(FileIndexResult::Remain), rows);
        assert_eq!(
            FileIndexResult::Skip.and(rows.clone()),
            FileIndexResult::Skip
        );
        assert_eq!(rows.and(FileIndexResult::Skip), FileIndexResult::Skip);
        assert_eq!(
            selection([1, 2, 3]).and(selection([2, 3, 4])),
            selection([2, 3])
        );
    }

    #[test]
    fn test_or() {
        let rows = selection([1, 2]);

        assert_eq!(
            FileIndexResult::Skip.or(FileIndexResult::Skip),
            FileIndexResult::Skip
        );
        assert_eq!(
            FileIndexResult::Skip.or(FileIndexResult::Remain),
            FileIndexResult::Remain
        );
        assert_eq!(
            FileIndexResult::Remain.or(rows.clone()),
            FileIndexResult::Remain
        );
        assert_eq!(
            rows.clone().or(FileIndexResult::Remain),
            FileIndexResult::Remain
        );
        assert_eq!(FileIndexResult::Skip.or(rows.clone()), rows);
        assert_eq!(rows.clone().or(FileIndexResult::Skip), rows);
        assert_eq!(
            selection([1, 2, 3]).or(selection([2, 3, 4])),
            selection([1, 2, 3, 4])
        );
    }
}
