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

use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum FileType {
    Meta,
    Data,
    BucketIndex,
    GlobalIndex,
    FileIndex,
}

impl FileType {
    pub(super) fn classify(path: &str) -> Self {
        let name = path.rsplit('/').next().unwrap_or(path);

        if name.starts_with("snapshot-")
            || name.starts_with("schema-")
            || name.starts_with("stat-")
            || name.starts_with("tag-")
            || name.starts_with("consumer-")
            || name.starts_with("service-")
            || name.contains("manifest")
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

        if name.starts_with("index-") {
            return Self::BucketIndex;
        }

        Self::Data
    }

    pub(super) fn is_mutable(path: &str) -> bool {
        let name = path.rsplit('/').next().unwrap_or(path);
        matches!(name, "LATEST" | "EARLIEST")
            || name.starts_with("tag-")
            || name.starts_with("consumer-")
            || name.starts_with("service-")
            || name.ends_with(".tmp")
            || name.contains(".tmp-")
            || name.contains(".tmp.")
    }

    pub(super) fn parse_whitelist(value: &str) -> HashSet<Self> {
        value
            .split(',')
            .filter_map(|name| match name.trim() {
                "meta" => Some(Self::Meta),
                "global-index" => Some(Self::GlobalIndex),
                "bucket-index" => Some(Self::BucketIndex),
                "data" => Some(Self::Data),
                "file-index" => Some(Self::FileIndex),
                "" => None,
                unknown => {
                    log::warn!(
                        "Unknown local-cache.whitelist value '{}'; supported values are \
                         meta, global-index, bucket-index, data, file-index",
                        unknown
                    );
                    None
                }
            })
            .collect()
    }
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
        ];

        for (path, expected) in cases {
            assert_eq!(FileType::classify(path), expected, "{path}");
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
        ] {
            assert!(FileType::is_mutable(path), "{path}");
        }
        assert!(!FileType::is_mutable(
            "s3://bucket/table/snapshot/snapshot-1"
        ));
    }

    #[test]
    fn test_file_type_parses_whitelist() {
        let whitelist =
            FileType::parse_whitelist(" meta,global-index, bucket-index,data,file-index,unknown ");

        assert_eq!(whitelist.len(), 5);
        for file_type in [
            FileType::Meta,
            FileType::GlobalIndex,
            FileType::BucketIndex,
            FileType::Data,
            FileType::FileIndex,
        ] {
            assert!(whitelist.contains(&file_type));
        }
    }
}
