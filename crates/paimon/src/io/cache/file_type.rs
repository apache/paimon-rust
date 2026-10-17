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

use crate::spec::MANIFEST_SIDECAR_SUFFIX;
use std::collections::HashSet;

// Suffix `.{uuid}.tmp` appended by Java `Path.createTempPath()`: 1 dot + 36-char UUID + `.tmp`.
const TEMP_FILE_SUFFIX_LEN: usize = 41;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum FileType {
    Meta,
    Data,
    BucketIndex,
    GlobalIndex,
    FileIndex,
}

impl FileType {
    const ALL: [Self; 5] = [
        Self::Meta,
        Self::Data,
        Self::BucketIndex,
        Self::GlobalIndex,
        Self::FileIndex,
    ];

    /// Mirrors Java `org.apache.paimon.utils.FileType#classify`, including its check order.
    pub(crate) fn classify(path: &str) -> Self {
        let mut segments = path.rsplit('/');
        let name = unwrap_temp_file_name(segments.next().unwrap_or(path));

        if name.starts_with("snapshot-")
            || name.starts_with("schema-")
            || name.starts_with("stat-")
            || name.starts_with("tag-")
            || name.starts_with("consumer-")
            || name.starts_with("service-")
        {
            return Self::Meta;
        }

        if name.ends_with(".index") {
            return if name.contains("global-index-") {
                Self::GlobalIndex
            } else {
                Self::FileIndex
            };
        }

        if name.contains("manifest") || name.ends_with(MANIFEST_SIDECAR_SUFFIX) {
            return Self::Meta;
        }

        if name.starts_with("index-") {
            return Self::BucketIndex;
        }

        if matches!(name, "LATEST" | "EARLIEST") || name.ends_with("_SUCCESS") {
            return Self::Meta;
        }

        if name.starts_with("changelog-") && segments.next() == Some("changelog") {
            return Self::Meta;
        }

        Self::Data
    }

    pub(crate) fn is_mutable(path: &str) -> bool {
        let name = path.rsplit('/').next().unwrap_or(path);
        // Iceberg-compatible `version-hint.text`, `retire-pending` and `v{N}.metadata.json`
        // (on tag changes) are rewritten in place.
        matches!(
            name,
            "LATEST" | "EARLIEST" | "version-hint.text" | "retire-pending"
        ) || name.ends_with(".metadata.json")
            || name.ends_with("_SUCCESS")
            || name.starts_with("tag-")
            || name.starts_with("consumer-")
            || name.starts_with("service-")
            || name.ends_with(".tmp")
            || name.contains(".tmp-")
            || name.contains(".tmp.")
    }

    pub(crate) fn parse_whitelist(value: &str) -> HashSet<Self> {
        value
            .split(',')
            .flat_map(|name| -> &'static [Self] {
                match name.trim() {
                    "*" => &Self::ALL,
                    "meta" => &[Self::Meta],
                    "global-index" => &[Self::GlobalIndex],
                    "bucket-index" => &[Self::BucketIndex],
                    "data" => &[Self::Data],
                    "file-index" => &[Self::FileIndex],
                    "" => &[],
                    unknown => {
                        log::warn!(
                            "Unknown local-cache.whitelist value '{}'; supported values are \
                             meta, global-index, bucket-index, data, file-index, \
                             or * for all of them",
                            unknown
                        );
                        &[]
                    }
                }
            })
            .copied()
            .collect()
    }
}

/// Returns `originalName` for a Java temp name `.{originalName}.{uuid}.tmp`, else `name`.
fn unwrap_temp_file_name(name: &str) -> &str {
    let bytes = name.as_bytes();
    if bytes.len() < TEMP_FILE_SUFFIX_LEN + 2 || bytes[0] != b'.' || !name.ends_with(".tmp") {
        return name;
    }
    let dot_before_uuid = bytes.len() - TEMP_FILE_SUFFIX_LEN;
    if bytes[dot_before_uuid] != b'.' {
        return name;
    }
    &name[1..dot_before_uuid]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_type_classifies_paimon_paths() {
        let cases = [
            ("s3://bucket/table/snapshot/snapshot-42", FileType::Meta),
            ("s3://bucket/table/schema/schema-3", FileType::Meta),
            ("s3://bucket/table/manifest/stat-1", FileType::Meta),
            (
                "s3://bucket/table/manifest/manifest-list-abc-0",
                FileType::Meta,
            ),
            (
                "s3://bucket/table/index/btree-global-index-abc.index",
                FileType::GlobalIndex,
            ),
            (
                "s3://bucket/table/index/vector-ivf-global-index-abc.index",
                FileType::GlobalIndex,
            ),
            ("s3://bucket/table/index/index-abc-0", FileType::BucketIndex),
            (
                "s3://bucket/table/data/data-abc.parquet.index",
                FileType::FileIndex,
            ),
            (
                "s3://bucket/table/bucket-0/data-abc.parquet",
                FileType::Data,
            ),
            (
                "s3://bucket/table/manifest/manifest-123e4567-e89b-12d3-a456-426614174000-0.avro.sidecar",
                FileType::Meta,
            ),
            // `.index` is checked before `manifest`, as in Java.
            (
                "s3://bucket/table/index/manifest-global-index-abc.index",
                FileType::GlobalIndex,
            ),
            ("s3://bucket/table/changelog/changelog-5", FileType::Meta),
            (
                "s3://bucket/table/bucket-0/changelog-123e4567-e89b-12d3-a456-426614174000-0.parquet",
                FileType::Data,
            ),
            (
                "s3://bucket/table/snapshot/.snapshot-13.123e4567-e89b-12d3-a456-426614174000.tmp",
                FileType::Meta,
            ),
            (
                "s3://bucket/table/changelog/.changelog-5.123e4567-e89b-12d3-a456-426614174000.tmp",
                FileType::Meta,
            ),
            (
                "s3://bucket/table/bucket-0/.data-abc.parquet.123e4567-e89b-12d3-a456-426614174000.tmp",
                FileType::Data,
            ),
        ];

        for (path, expected) in cases {
            assert_eq!(FileType::classify(path), expected, "{path}");
        }
    }

    #[test]
    fn test_file_type_classifies_mutable_markers_as_meta() {
        for path in [
            "s3://bucket/table/snapshot/LATEST",
            "s3://bucket/table/snapshot/EARLIEST",
            "s3://bucket/table/changelog/LATEST",
            "s3://bucket/table/changelog/EARLIEST",
            "s3://bucket/table/branch/branch-dev/snapshot/LATEST",
            "s3://bucket/table/branch/branch-dev/snapshot/EARLIEST",
            "s3://bucket/table/dt=1/_SUCCESS",
            "s3://bucket/table/tag/tag-success-file/t1_SUCCESS",
            "s3://bucket/table/snapshot/.snapshot-13.123e4567-e89b-12d3-a456-426614174000.tmp",
        ] {
            assert_eq!(FileType::classify(path), FileType::Meta, "{path}");
            assert!(FileType::is_mutable(path), "{path}");
        }
    }

    #[test]
    fn test_file_type_bypasses_mutable_paths() {
        for path in [
            "s3://bucket/table/snapshot/LATEST",
            "s3://bucket/table/snapshot/EARLIEST",
            "s3://bucket/table/tag/tag-production",
            "s3://bucket/table/consumer/consumer-job",
            "s3://bucket/table/service/service-api",
            "s3://bucket/table/snapshot/.snapshot-1.123e4567-e89b-12d3-a456-426614174000.tmp",
            "s3://bucket/table/snapshot/snapshot-1.tmp-123e4567-e89b-12d3-a456-426614174000",
            "s3://bucket/table/dt=1/_SUCCESS",
            "s3://bucket/table/tag/tag-success-file/t1_SUCCESS",
            "s3://bucket/table/metadata/version-hint.text",
            "s3://bucket/table/metadata/retire-pending",
            "s3://bucket/table/metadata/v3.metadata.json",
        ] {
            assert!(FileType::is_mutable(path), "{path}");
        }
        for path in [
            "s3://bucket/table/snapshot/snapshot-1",
            "s3://bucket/table/changelog/changelog-5",
            "s3://bucket/table/metadata/snap-1-1-123e4567-e89b-12d3-a456-426614174000.avro",
        ] {
            assert!(!FileType::is_mutable(path), "{path}");
        }
    }

    #[test]
    fn test_file_type_parses_whitelist() {
        let all = HashSet::from([
            FileType::Meta,
            FileType::GlobalIndex,
            FileType::BucketIndex,
            FileType::Data,
            FileType::FileIndex,
        ]);

        for value in [
            " meta,global-index, bucket-index,data,file-index,unknown ",
            "*",
            " meta , * ",
        ] {
            assert_eq!(FileType::parse_whitelist(value), all, "{value}");
        }
        assert_eq!(
            FileType::parse_whitelist("meta,global-index"),
            HashSet::from([FileType::Meta, FileType::GlobalIndex])
        );
    }
}
