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

//! Format Table partition names, paths and values, shared by the scan, the catalog
//! registrations it reads and the SQL statements that administer them.

use std::collections::HashMap;

use chrono::NaiveDate;

use crate::spec::{escape_path_name, DataType, Datum};

const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Generates canonical names and physical paths for Format Table partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatTablePartitionPaths {
    partition_keys: Vec<String>,
    only_value_in_path: bool,
}

impl FormatTablePartitionPaths {
    /// Create a helper for the declared partition-key order and physical layout.
    pub fn new<I, S>(partition_keys: I, only_value_in_path: bool) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            partition_keys: partition_keys.into_iter().map(Into::into).collect(),
            only_value_in_path,
        }
    }

    /// Return the canonical logical partition name (`key=value/...`).
    pub fn partition_name(&self, spec: &HashMap<String, String>) -> crate::Result<String> {
        let values = self.ordered_values(spec)?;
        Ok(self
            .partition_keys
            .iter()
            .zip(values)
            .map(|(key, value)| format!("{}={}", escape_path_name(key), escape_path_name(value)))
            .collect::<Vec<_>>()
            .join("/"))
    }

    /// Build the partition-name pattern that selects the partitions whose leading values
    /// are `leading_values`, to push down to a partition-managing catalog.
    ///
    /// Pattern contract, shared by every engine talking to the catalog: partition names are
    /// the escaped `key=value` form joined by `/`, `%` is the only wildcard and has no
    /// escape sequence, and `_` stays literal. Values covering every partition key give the
    /// one exact name; a shorter prefix is suffixed with `%`.
    ///
    /// `None` means pushdown has to be skipped and the caller must list every partition:
    /// there are no leading values, one of them is blank, or escaping produced a literal
    /// `%` that the contract cannot express.
    ///
    /// Mirrors Java `PartitionPathUtils.buildPartitionNamePrefixPattern`.
    pub(crate) fn name_prefix_pattern(&self, leading_values: &[String]) -> Option<String> {
        if leading_values.is_empty() || leading_values.len() > self.partition_keys.len() {
            return None;
        }
        let mut segments = Vec::with_capacity(leading_values.len());
        for (key, value) in self.partition_keys.iter().zip(leading_values) {
            if value.trim().is_empty() {
                return None;
            }
            segments.push(format!(
                "{}={}",
                escape_path_name(key),
                escape_path_name(value)
            ));
        }
        let pattern = segments.join("/");
        if pattern.contains('%') {
            return None;
        }
        if leading_values.len() == self.partition_keys.len() {
            Some(pattern)
        } else {
            Some(format!("{pattern}/%"))
        }
    }

    /// Return the physical partition path relative to the table location.
    pub fn relative_path(&self, spec: &HashMap<String, String>) -> crate::Result<String> {
        if !self.only_value_in_path {
            return self.partition_name(spec);
        }
        Ok(self
            .ordered_values(spec)?
            .into_iter()
            .map(escape_path_name)
            .collect::<Vec<_>>()
            .join("/"))
    }

    fn ordered_values<'a>(&self, spec: &'a HashMap<String, String>) -> crate::Result<Vec<&'a str>> {
        if spec.len() != self.partition_keys.len() {
            return Err(crate::Error::DataInvalid {
                message: self.invalid_partition_keys_message(spec),
                source: None,
            });
        }

        let mut values = Vec::with_capacity(self.partition_keys.len());
        for key in &self.partition_keys {
            let Some(value) = spec.get(key).map(String::as_str) else {
                return Err(crate::Error::DataInvalid {
                    message: self.invalid_partition_keys_message(spec),
                    source: None,
                });
            };
            if value.is_empty() || (self.only_value_in_path && matches!(value, "." | "..")) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Partition value {value:?} cannot be used as a partition path component"
                    ),
                    source: None,
                });
            }
            values.push(value);
        }
        Ok(values)
    }

    fn invalid_partition_keys_message(&self, spec: &HashMap<String, String>) -> String {
        let mut actual_keys = spec.keys().collect::<Vec<_>>();
        actual_keys.sort();
        format!(
            "Partition spec must contain exactly keys {:?}, but contains {actual_keys:?}",
            self.partition_keys
        )
    }
}

/// Parse a raw Format Table partition value from a path or catalog registration.
pub fn parse_format_partition_value(value: &str, data_type: &DataType) -> Option<Datum> {
    match data_type {
        DataType::Boolean(_) => parse_partition_bool(value).map(Datum::Bool),
        DataType::TinyInt(_) => value.parse::<i8>().ok().map(Datum::TinyInt),
        DataType::SmallInt(_) => value.parse::<i16>().ok().map(Datum::SmallInt),
        DataType::Int(_) => value.parse::<i32>().ok().map(Datum::Int),
        DataType::BigInt(_) => value.parse::<i64>().ok().map(Datum::Long),
        DataType::Char(_) | DataType::VarChar(_) => Some(Datum::String(value.to_string())),
        DataType::Date(_) => parse_partition_date(value).map(Datum::Date),
        DataType::Time(_) => value.parse::<i32>().ok().map(Datum::Time),
        _ => None,
    }
}

/// Format a typed value for Format Table partition metadata and paths.
pub fn format_partition_value(
    datum: &Datum,
    data_type: &DataType,
    default_partition_name: &str,
    legacy_partition_name: bool,
) -> Option<String> {
    match (datum, data_type) {
        (Datum::Bool(value), DataType::Boolean(_)) => Some(value.to_string()),
        (Datum::TinyInt(value), DataType::TinyInt(_)) => Some(value.to_string()),
        (Datum::SmallInt(value), DataType::SmallInt(_)) => Some(value.to_string()),
        (Datum::Int(value), DataType::Int(_)) => Some(value.to_string()),
        (Datum::Long(value), DataType::BigInt(_)) => Some(value.to_string()),
        (Datum::String(value), DataType::Char(_) | DataType::VarChar(_)) => {
            if value.trim().is_empty() {
                Some(default_partition_name.to_string())
            } else {
                Some(value.clone())
            }
        }
        (Datum::Date(value), DataType::Date(_)) => {
            if legacy_partition_name {
                Some(value.to_string())
            } else {
                format_partition_date(*value)
            }
        }
        (Datum::Time(value), DataType::Time(_)) => Some(value.to_string()),
        _ => None,
    }
}

/// Accept the boolean spellings Java accepts, so a partition another engine registered as
/// `TRUE`, `t`, `yes` or `1` reads back here instead of failing as invalid metadata.
///
/// Mirrors Java `BinaryStringUtils.toBoolean`.
fn parse_partition_bool(value: &str) -> Option<bool> {
    const TRUE_VALUES: [&str; 5] = ["t", "true", "y", "yes", "1"];
    const FALSE_VALUES: [&str; 5] = ["f", "false", "n", "no", "0"];
    if TRUE_VALUES
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
    {
        return Some(true);
    }
    if FALSE_VALUES
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
    {
        return Some(false);
    }
    None
}

fn parse_partition_date(value: &str) -> Option<i32> {
    if let Ok(epoch_days) = value.parse::<i32>() {
        return Some(epoch_days);
    }
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAYS_FROM_CE)?;
    date.signed_duration_since(epoch).num_days().try_into().ok()
}

fn format_partition_date(epoch_days: i32) -> Option<String> {
    NaiveDate::from_num_days_from_ce_opt(epoch_days.checked_add(UNIX_EPOCH_DAYS_FROM_CE)?)
        .map(|date| date.format("%Y-%m-%d").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BooleanType, DateType};

    #[test]
    fn test_parse_format_partition_value() {
        assert_eq!(
            parse_format_partition_value("true", &DataType::Boolean(BooleanType::new())),
            Some(Datum::Bool(true))
        );
        assert_eq!(
            parse_format_partition_value("2026-07-22", &DataType::Date(DateType::new())),
            Some(Datum::Date(20_656))
        );
        assert_eq!(
            parse_format_partition_value("20656", &DataType::Date(DateType::new())),
            Some(Datum::Date(20_656))
        );
    }

    #[test]
    fn test_parse_format_partition_bool_accepts_every_java_spelling() {
        let boolean = DataType::Boolean(BooleanType::new());
        for value in ["t", "T", "true", "TRUE", "True", "y", "YES", "1"] {
            assert_eq!(
                parse_format_partition_value(value, &boolean),
                Some(Datum::Bool(true)),
                "{value} should read as true"
            );
        }
        for value in ["f", "F", "false", "FALSE", "False", "n", "NO", "0"] {
            assert_eq!(
                parse_format_partition_value(value, &boolean),
                Some(Datum::Bool(false)),
                "{value} should read as false"
            );
        }
        for value in ["", "2", "tru", "yes please", "null"] {
            assert_eq!(
                parse_format_partition_value(value, &boolean),
                None,
                "{value} is not a boolean"
            );
        }
    }

    #[test]
    fn test_partition_paths_escape_values_and_honor_layout() {
        let spec = HashMap::from([
            ("dt".to_string(), "2026/07=22".to_string()),
            ("hour".to_string(), "10".to_string()),
        ]);

        let keyed = FormatTablePartitionPaths::new(["dt", "hour"], false);
        assert_eq!(
            keyed.partition_name(&spec).unwrap(),
            "dt=2026%2F07%3D22/hour=10"
        );
        assert_eq!(
            keyed.relative_path(&spec).unwrap(),
            "dt=2026%2F07%3D22/hour=10"
        );

        let value_only = FormatTablePartitionPaths::new(["dt", "hour"], true);
        assert_eq!(
            value_only.partition_name(&spec).unwrap(),
            "dt=2026%2F07%3D22/hour=10"
        );
        assert_eq!(
            value_only.relative_path(&spec).unwrap(),
            "2026%2F07%3D22/10"
        );

        // A spec that does not name exactly the partition keys has no path.
        let missing_hour = HashMap::from([("dt".to_string(), "a".to_string())]);
        assert!(keyed.relative_path(&missing_hour).is_err());
        let traversal = HashMap::from([
            ("dt".to_string(), "..".to_string()),
            ("hour".to_string(), "10".to_string()),
        ]);
        assert!(value_only.relative_path(&traversal).is_err());
    }

    #[test]
    fn test_name_prefix_pattern() {
        let paths = FormatTablePartitionPaths::new(["dt".to_string(), "hh".to_string()], false);

        // A complete prefix names one partition, a shorter one takes the wildcard.
        assert_eq!(
            paths.name_prefix_pattern(&["20260722".to_string(), "10".to_string()]),
            Some("dt=20260722/hh=10".to_string())
        );
        assert_eq!(
            paths.name_prefix_pattern(&["20260722".to_string()]),
            Some("dt=20260722/%".to_string())
        );

        // The physical layout never changes the pattern: catalog names are always key=value.
        let value_only = FormatTablePartitionPaths::new(["dt".to_string(), "hh".to_string()], true);
        assert_eq!(
            value_only.name_prefix_pattern(&["20260722".to_string()]),
            Some("dt=20260722/%".to_string())
        );

        // Nothing to push down.
        assert_eq!(paths.name_prefix_pattern(&[]), None);
        assert_eq!(paths.name_prefix_pattern(&["  ".to_string()]), None);
        assert_eq!(
            paths.name_prefix_pattern(&["a".to_string(), "b".to_string(), "c".to_string()]),
            None
        );

        // Escaping a value would inject the pattern's only wildcard, so pushdown is skipped
        // rather than silently widened.
        assert_eq!(paths.name_prefix_pattern(&["2026/07".to_string()]), None);
    }
}
