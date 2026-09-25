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

//! Resolve and validate the locations of catalog-managed Format Table partitions.
//! A catalog location is untrusted metadata: its path must be checked before a
//! scan opens files, and one partition must never claim another one's data.

use std::collections::HashMap;

use url::Url;

use crate::spec::Partition;
use crate::Error;

type SpecKey = Vec<(String, String)>;
type FileSystemKey = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoragePath {
    scheme: String,
    authority: String,
    path: String,
    location: String,
}

impl StoragePath {
    fn parse(location: &str, custom: bool) -> crate::Result<Self> {
        validate_location_text(location)?;
        let decoded = if custom {
            let decoded = decode_percent_once(location)?;
            if decoded.contains('%') {
                return Err(invalid_location());
            }
            validate_location_text(&decoded)?;
            decoded
        } else {
            location.to_string()
        };

        let url = if decoded.starts_with('/') {
            Url::from_file_path(&decoded).map_err(|_| invalid_location())?
        } else {
            Url::parse(&decoded).map_err(|_| invalid_location())?
        };
        let abfs = matches!(url.scheme().to_ascii_lowercase().as_str(), "abfs" | "abfss");
        if url.cannot_be_a_base()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path().is_empty()
            || url.path() == "/"
            || (!url.username().is_empty() && (!abfs || url.username().contains(':')))
            || url.password().is_some()
        {
            return Err(invalid_location());
        }

        let scheme = match url.scheme().to_ascii_lowercase().as_str() {
            "s3a" | "s3n" => "s3".to_string(),
            "abfss" => "abfs".to_string(),
            other => other.to_string(),
        };
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        let authority = if abfs && !url.username().is_empty() {
            format!("{}@{host}", url.username().to_ascii_lowercase())
        } else {
            host
        };
        if custom
            && ((scheme == "file" && !authority.is_empty())
                || (scheme != "file" && scheme != "hdfs" && authority.is_empty())
                || (scheme == "hdfs" && authority.is_empty())
                || scheme == "viewfs")
        {
            return Err(invalid_location());
        }
        let authority = match url.port() {
            Some(port) if scheme == "hdfs" && port == 8020 => authority,
            Some(port) => format!("{authority}:{port}"),
            None => authority,
        };
        // Hadoop Path collapses redundant separators. Ownership must use the
        // same directory identity or `a//b` can evade an `a/b` overlap check.
        let path = format!(
            "/{}",
            url.path()
                .split('/')
                .filter(|segment| !segment.is_empty())
                .collect::<Vec<_>>()
                .join("/")
        );
        Ok(Self {
            scheme,
            authority,
            path,
            location: url.to_string().trim_end_matches('/').to_string(),
        })
    }

    fn file_system(&self) -> FileSystemKey {
        (self.scheme.clone(), self.authority.clone())
    }

    fn same_directory(&self, other: &Self) -> bool {
        self.file_system() == other.file_system() && self.path == other.path
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.file_system() == other.file_system()
            && (self.path == other.path
                || self.path.starts_with(&format!("{}/", other.path))
                || other.path.starts_with(&format!("{}/", self.path)))
    }
}

#[derive(Default)]
struct OwnershipNode {
    owned: bool,
    children: HashMap<String, OwnershipNode>,
}

/// Mirrors Java's `FormatTablePartitionPathResolver` for paths the Rust
/// `FileIO` can address. A duplicate catalog row is harmless only if it names
/// the same spec and the same canonical location.
pub(super) struct FormatPartitionLocations {
    table_root: StoragePath,
    by_spec: HashMap<SpecKey, StoragePath>,
    roots: HashMap<FileSystemKey, OwnershipNode>,
}

impl FormatPartitionLocations {
    pub(super) fn new(table_path: &str) -> crate::Result<Self> {
        Ok(Self {
            table_root: StoragePath::parse(table_path, false)?,
            by_spec: HashMap::new(),
            roots: HashMap::new(),
        })
    }

    /// Return `None` for an identical duplicate registry row, otherwise the
    /// path to list. An explicit path spelling the default directory is a
    /// rebind, not an external location.
    pub(super) fn resolve(
        &mut self,
        partition: &Partition,
        default_path: &str,
    ) -> crate::Result<Option<String>> {
        let default = StoragePath::parse(default_path, false)?;
        let custom = partition
            .options
            .as_ref()
            .and_then(|options| options.get("path"));
        let selected = if let Some(custom) = custom {
            // The default path can contain escaped partition values. Compare
            // that spelling before decoding a user-supplied external location.
            let default_spelling = StoragePath::parse(custom, false);
            let parsed = if default_spelling
                .as_ref()
                .is_ok_and(|path| path.same_directory(&default))
            {
                Ok(default.clone())
            } else {
                StoragePath::parse(custom, true)
            }
            .map_err(|source| Error::DataInvalid {
                message: format!(
                    "Catalog returned an invalid custom location for partition {:?}",
                    partition.spec
                ),
                source: Some(Box::new(source)),
            })?;
            if parsed.same_directory(&default) {
                default.clone()
            } else {
                if parsed.overlaps(&self.table_root) {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "Custom location for partition {:?} overlaps the Format Table directory",
                            partition.spec
                        ),
                        source: None,
                    });
                }
                parsed
            }
        } else {
            default.clone()
        };

        let spec = spec_key(&partition.spec);
        if let Some(previous) = self.by_spec.get(&spec) {
            if previous.same_directory(&selected) {
                return Ok(None);
            }
            return Err(overlapping_locations());
        }
        let node = self.roots.entry(selected.file_system()).or_default();
        let mut node = node;
        for segment in selected.path.trim_start_matches('/').split('/') {
            if node.owned {
                return Err(overlapping_locations());
            }
            node = node.children.entry(segment.to_string()).or_default();
        }
        if node.owned || !node.children.is_empty() {
            return Err(overlapping_locations());
        }
        node.owned = true;
        let location = if selected.same_directory(&default) {
            default_path.to_string()
        } else {
            selected.location.clone()
        };
        self.by_spec.insert(spec, selected);
        Ok(Some(location))
    }
}

fn spec_key(spec: &HashMap<String, String>) -> SpecKey {
    let mut key = spec
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    key.sort();
    key
}

fn validate_location_text(location: &str) -> crate::Result<()> {
    if location.is_empty()
        || location.trim() != location
        || location
            .chars()
            .any(|c| matches!(c, '?' | '#' | '\\') || c.is_control())
        || location.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(invalid_location());
    }
    Ok(())
}

fn decode_percent_once(value: &str) -> crate::Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(invalid_location());
            }
            let high = (bytes[index + 1] as char)
                .to_digit(16)
                .ok_or_else(invalid_location)?;
            let low = (bytes[index + 2] as char)
                .to_digit(16)
                .ok_or_else(invalid_location)?;
            decoded.push(((high << 4) | low) as u8);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| invalid_location())
}

fn invalid_location() -> Error {
    Error::DataInvalid {
        message: "Invalid custom Format Table partition location".into(),
        source: None,
    }
}

fn overlapping_locations() -> Error {
    Error::DataInvalid {
        message: "Catalog returned overlapping locations for Format Table partitions".into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(name: &str, path: Option<&str>) -> Partition {
        Partition {
            spec: HashMap::from([("dt".to_string(), name.to_string())]),
            record_count: Partition::UNKNOWN,
            file_size_in_bytes: Partition::UNKNOWN,
            file_count: Partition::UNKNOWN,
            last_file_creation_time: Partition::UNKNOWN,
            total_buckets: Partition::UNKNOWN_TOTAL_BUCKETS,
            done: false,
            created_at: None,
            created_by: None,
            updated_at: None,
            updated_by: None,
            options: path.map(|path| HashMap::from([("path".to_string(), path.to_string())])),
        }
    }

    fn default(name: &str) -> String {
        format!("file:///warehouse/events/dt={name}")
    }

    #[test]
    fn custom_partition_reads_external_location_and_default_rebind() {
        let mut locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
        assert_eq!(
            locations
                .resolve(&partition("a", Some("file:/external/a")), &default("a"))
                .unwrap(),
            Some("file:///external/a".to_string())
        );
        assert_eq!(
            locations
                .resolve(
                    &partition("b", Some("file:/warehouse/events/dt=b")),
                    &default("b")
                )
                .unwrap(),
            Some(default("b"))
        );
        assert_eq!(
            locations
                .resolve(&partition("c", None), &default("c"))
                .unwrap(),
            Some(default("c"))
        );
    }

    #[test]
    fn duplicate_registry_rows_must_have_the_same_spec_and_location() {
        let mut locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
        let a = partition("a", Some("file:/external/a"));
        assert!(locations.resolve(&a, &default("a")).unwrap().is_some());
        assert!(locations.resolve(&a, &default("a")).unwrap().is_none());
        let conflicting = partition("a", Some("file:/external/other"));
        assert!(locations
            .resolve(&conflicting, &default("a"))
            .unwrap_err()
            .to_string()
            .contains("overlapping locations"));
    }

    #[test]
    fn custom_location_cannot_claim_table_data_or_another_partition() {
        for invalid in [
            "file:/warehouse/events",
            "file:/warehouse/events/dt=b",
            "file:/warehouse/events/dt=b/child",
            "file:/warehouse",
        ] {
            let mut locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
            let error = locations
                .resolve(&partition("a", Some(invalid)), &default("a"))
                .unwrap_err();
            assert!(error.to_string().contains("overlaps"), "{invalid}: {error}");
        }

        let mut locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
        locations
            .resolve(
                &partition("a", Some("file:/external/parent")),
                &default("a"),
            )
            .unwrap();
        let error = locations
            .resolve(
                &partition("b", Some("file:/external/parent/child")),
                &default("b"),
            )
            .unwrap_err();
        assert!(error.to_string().contains("overlapping locations"));
        // A textual prefix without a path-segment boundary is a different directory.
        assert!(locations
            .resolve(
                &partition("c", Some("file:/external/parent-sibling")),
                &default("c"),
            )
            .unwrap()
            .is_some());
    }

    #[test]
    fn invalid_custom_locations_fail_before_listing() {
        for invalid in [
            "file:/external/../secret",
            "file:/external/%2e%2e/secret",
            "file:/external/%252e%252e/secret",
            "file:/external/%2F..%2Fsecret",
            "file:/external/%ZZ",
            "file:/external/%FF",
            "file:/external/a?query=1",
            "file:/external/a#fragment",
            "file:/external/\\secret",
            "file:/external/a\n",
            " file:/external/a",
            "file://remote-host/external/a",
            "relative/a",
            "viewfs://mount/external/a",
            "s3:///external/a",
        ] {
            let mut locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
            let error = locations
                .resolve(&partition("a", Some(invalid)), &default("a"))
                .unwrap_err();
            assert!(
                error.to_string().contains("invalid custom location"),
                "{invalid}: {error}"
            );
        }
    }

    #[test]
    fn ownership_uses_canonical_file_system_identity() {
        let mut locations = FormatPartitionLocations::new("s3://warehouse/events").unwrap();
        locations
            .resolve(
                &partition("a", Some("s3a://bucket/external/a")),
                "s3://warehouse/events/dt=a",
            )
            .unwrap();
        let error = locations
            .resolve(
                &partition("b", Some("s3://bucket/external/a/child")),
                "s3://warehouse/events/dt=b",
            )
            .unwrap_err();
        assert!(error.to_string().contains("overlapping locations"));
    }

    #[test]
    fn abfs_container_userinfo_is_part_of_file_system_identity() {
        let table = "abfss://warehouse@account.dfs.core.windows.net/events";
        let mut locations = FormatPartitionLocations::new(table).unwrap();
        let first = "abfs://archive@account.dfs.core.windows.net/external/a";
        assert!(locations
            .resolve(
                &partition("a", Some(first)),
                "abfss://warehouse@account.dfs.core.windows.net/events/dt=a",
            )
            .unwrap()
            .is_some());
        let same_container = "abfss://archive@ACCOUNT.dfs.core.windows.net/external/a/child";
        let error = locations
            .resolve(
                &partition("b", Some(same_container)),
                "abfss://warehouse@account.dfs.core.windows.net/events/dt=b",
            )
            .unwrap_err();
        assert!(error.to_string().contains("overlapping locations"));
        let another_container = "abfss://other@account.dfs.core.windows.net/external/a/child";
        assert!(locations
            .resolve(
                &partition("c", Some(another_container)),
                "abfss://warehouse@account.dfs.core.windows.net/events/dt=c",
            )
            .unwrap()
            .is_some());
    }

    #[test]
    fn redundant_separators_and_hdfs_default_port_cannot_evade_ownership() {
        let mut file_locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
        file_locations
            .resolve(&partition("a", Some("file:/external/a//b")), &default("a"))
            .unwrap();
        assert!(file_locations
            .resolve(
                &partition("b", Some("file:/external/a/b/child")),
                &default("b"),
            )
            .unwrap_err()
            .to_string()
            .contains("overlapping locations"));

        let mut hdfs_locations =
            FormatPartitionLocations::new("hdfs://NameNode:8020/warehouse/events").unwrap();
        hdfs_locations
            .resolve(
                &partition("a", Some("hdfs://NameNode/external/a")),
                "hdfs://NameNode/warehouse/events/dt=a",
            )
            .unwrap();
        assert!(hdfs_locations
            .resolve(
                &partition("b", Some("hdfs://namenode:8020/external/a/child")),
                "hdfs://NameNode/warehouse/events/dt=b",
            )
            .unwrap_err()
            .to_string()
            .contains("overlapping locations"));
    }

    #[test]
    fn escaped_default_partition_value_stays_one_path_segment() {
        let mut locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
        let escaped_default = "file:///warehouse/events/dt=a%2Fb";
        assert_eq!(
            locations
                .resolve(&partition("a/b", Some(escaped_default)), escaped_default)
                .unwrap(),
            Some(escaped_default.to_string())
        );
        // The external path below the decoded spelling is a different
        // directory from the single escaped partition segment.
        assert!(locations
            .resolve(
                &partition("other", Some("file:/external/a/b")),
                &default("other"),
            )
            .unwrap()
            .is_some());
    }

    #[test]
    fn rejects_credentials_outside_abfs_and_invalid_abfs_userinfo() {
        for invalid in [
            "s3://user@bucket/external/a",
            "file://user@localhost/external/a",
            "abfs://container:password@account/external/a",
        ] {
            let mut locations = FormatPartitionLocations::new("file:///warehouse/events").unwrap();
            assert!(locations
                .resolve(&partition("a", Some(invalid)), &default("a"))
                .unwrap_err()
                .to_string()
                .contains("invalid custom location"));
        }
    }
}
