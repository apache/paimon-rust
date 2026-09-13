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

use std::collections::HashMap;
#[cfg(not(windows))]
use std::path::Path;

use paimon::io::FileIO;
use paimon::table::FormatTablePartitionPaths;

#[cfg(not(windows))]
async fn discover_partitions(
    root: &Path,
    partition_keys: &[&str],
    only_value_in_path: bool,
    default_partition_name: &str,
) -> paimon::Result<Vec<HashMap<String, String>>> {
    let table_path = format!("file://{}", root.display());
    let file_io = FileIO::from_path(&table_path)?.build()?;
    FormatTablePartitionPaths::new(partition_keys.iter().copied(), only_value_in_path)
        .discover(&file_io, &table_path, default_partition_name)
        .await
}

#[cfg(not(windows))]
#[tokio::test]
async fn discover_format_partitions_returns_complete_sorted_specs() {
    let tmp = tempfile::tempdir().unwrap();
    for relative in [
        "dt=2026-07-22/hour=10",
        "dt=2026-07-21/hour=09",
        "dt=2026-07-20",
        ".staging/dt=2026-07-19/hour=08",
    ] {
        std::fs::create_dir_all(tmp.path().join(relative)).unwrap();
    }

    let partitions =
        discover_partitions(tmp.path(), &["dt", "hour"], false, "__DEFAULT_PARTITION__")
            .await
            .unwrap();

    assert_eq!(
        partitions,
        vec![
            HashMap::from([
                ("dt".to_string(), "2026-07-21".to_string()),
                ("hour".to_string(), "09".to_string()),
            ]),
            HashMap::from([
                ("dt".to_string(), "2026-07-22".to_string()),
                ("hour".to_string(), "10".to_string()),
            ]),
        ]
    );
}

#[cfg(not(windows))]
#[tokio::test]
async fn discover_format_partitions_treats_missing_root_as_empty() {
    let tmp = tempfile::tempdir().unwrap();

    let partitions = discover_partitions(
        &tmp.path().join("missing"),
        &["dt"],
        false,
        "__DEFAULT_PARTITION__",
    )
    .await
    .unwrap();

    assert!(partitions.is_empty());
}

#[cfg(not(windows))]
#[tokio::test]
async fn discover_value_only_partitions_rejects_parent_traversal_value() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("%2E%2E")).unwrap();

    let error = discover_partitions(tmp.path(), &["dt"], true, "__DEFAULT_PARTITION__")
        .await
        .unwrap_err();

    assert!(
        error.to_string().contains(".."),
        "expected traversal value in error, got: {error}"
    );
}

#[cfg(not(windows))]
#[tokio::test]
async fn discover_value_only_partitions_keeps_hidden_default_directory() {
    for (default_name, directory, ignored_directory) in [
        ("__DEFAULT_PARTITION__", "__DEFAULT_PARTITION__", None),
        (".NULL", ".NULL", Some(".staging")),
        ("_NULL/%NA", "_NULL%2F%25NA", Some("_temporary")),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(directory)).unwrap();
        if let Some(ignored_directory) = ignored_directory {
            std::fs::create_dir_all(tmp.path().join(ignored_directory)).unwrap();
        }

        let partitions = discover_partitions(tmp.path(), &["dt"], true, default_name)
            .await
            .unwrap();

        assert_eq!(
            partitions,
            vec![HashMap::from([(
                "dt".to_string(),
                default_name.to_string()
            )])],
            "{default_name}"
        );
    }
}
