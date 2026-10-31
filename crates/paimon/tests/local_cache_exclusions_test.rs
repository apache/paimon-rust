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

#![cfg(feature = "storage-fs")]

use paimon::catalog::FileSystemCatalog;
use paimon::io::FileRead;
use paimon::{CatalogOptions, Error, Options};

async fn assert_cache_selection(
    whitelist: &str,
    excluded_extensions: Option<&str>,
    files: &[(&str, bool)],
) {
    let mut mismatches = Vec::new();
    for backend in ["memory", "disk", "shared"] {
        for range_read in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let warehouse = directory.path().join("warehouse");
            std::fs::create_dir(&warehouse).unwrap();
            let mut options = Options::new();
            options.set(CatalogOptions::WAREHOUSE, warehouse.to_string_lossy());
            options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
            options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "4");
            options.set(CatalogOptions::LOCAL_CACHE_WHITELIST, whitelist);
            if backend != "memory" {
                options.set(
                    CatalogOptions::LOCAL_CACHE_DIR,
                    directory.path().join("cache").to_string_lossy(),
                );
            }
            if backend == "shared" {
                options.set(CatalogOptions::LOCAL_CACHE_SHARED, "true");
                options.set(CatalogOptions::LOCAL_CACHE_MAX_SIZE, "1 MiB");
            }
            if let Some(extensions) = excluded_extensions {
                options.set(CatalogOptions::LOCAL_CACHE_EXCLUDE_EXTENSIONS, extensions);
            }
            let catalog = FileSystemCatalog::new(options).unwrap();
            for &(name, expected_cached) in files {
                let path = warehouse.join(name);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                // Only block-cache routing is under test, not format decoding.
                std::fs::write(&path, b"abcdefgh").unwrap();
                // Cache classification uses slash-separated FileIO paths on every platform.
                let io_path = path.to_str().unwrap().replace('\\', "/");
                let input = catalog.file_io().new_input(&io_path).unwrap();
                let expected: &[u8] = if range_read { b"bcdefg" } else { b"abcdefgh" };
                let cold = if range_read {
                    input.reader().await.unwrap().read(1..7).await.unwrap()
                } else {
                    input.read().await.unwrap()
                };
                assert_eq!(cold.as_ref(), expected);

                // Bypass FileIO invalidation. Keep the source available for shared-cache stat.
                std::fs::write(&path, b"12345678").unwrap();
                let replacement: &[u8] = if range_read { b"234567" } else { b"12345678" };
                let warm = if range_read {
                    async { input.reader().await?.read(1..7).await }.await
                } else {
                    input.read().await
                };
                let warm = warm.unwrap();
                assert!(warm.as_ref() == expected || warm.as_ref() == replacement);
                let cached = warm.as_ref() == expected;
                if cached != expected_cached {
                    mismatches.push(format!(
                        "backend={backend}, range_read={range_read}, whitelist={whitelist}, \
                         excluded_extensions={excluded_extensions:?}, file={name}: \
                         expected cached={expected_cached}, actual cached={cached}"
                    ));
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[tokio::test]
async fn test_missing_or_empty_exclusions_preserve_data_cache() {
    for exclusions in [None, Some(""), Some(" , , ")] {
        assert_cache_selection(
            "meta,global-index,data",
            exclusions,
            &[("data.parquet", true), ("data.blob", true)],
        )
        .await;
    }
}

#[tokio::test]
async fn test_structured_data_cache_excludes_dedicated_files() {
    assert_cache_selection(
        "meta,global-index,structured-data",
        None,
        &[
            ("data.parquet", true),
            ("data.unknown", true),
            ("data.blob", false),
            ("data.video", false),
            ("data.vector.vortex", false),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_issue_971_excluded_blob_bypasses_cache() {
    assert_cache_selection(
        "meta,global-index,data",
        Some("blob"),
        &[("data.parquet", true), ("data.blob", false)],
    )
    .await;
}

#[tokio::test]
async fn test_exclusions_normalize_entries_and_match_only_the_last_extension() {
    assert_cache_selection(
        "*",
        Some(" blob, .VIDEO, , BLOB "),
        &[
            ("data.blob", false),
            ("data.BLOB", false),
            ("data.video", false),
            ("data.VIDEO", false),
            ("data.parquet.blob", false),
            ("data.blob.gz", true),
            ("data.blob.index", true),
            ("data.blobber", true),
            ("dir.blob/data.parquet", true),
            ("blob", true),
            (".blob", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_exclusions_override_whitelisted_metadata_and_indexes() {
    assert_cache_selection(
        "*",
        Some("blob,index,gz"),
        &[
            ("snapshot-1.blob", false),
            ("global-index-1.index", false),
            ("data.parquet.index", false),
            ("data.csv.gz", false),
            ("manifest-1.avro", true),
            ("snapshot-1", true),
            ("data.csv", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_exclusions_do_not_expand_the_whitelist() {
    assert_cache_selection(
        "meta",
        Some("blob"),
        &[
            ("snapshot-1", true),
            ("data.parquet", false),
            ("data.blob", false),
            ("global-index-1.index", false),
        ],
    )
    .await;
    assert_cache_selection(
        "structured-data",
        Some("parquet"),
        &[
            ("data.parquet", false),
            ("data.unknown", true),
            ("data.blob", false),
            ("snapshot-1", false),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_exclusions_bypass_existing_blocks_and_preserve_invalidation() {
    for shared in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.blob");
        let io_path = path.to_str().unwrap().replace('\\', "/");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let mut options = Options::new();
        options.set(
            CatalogOptions::WAREHOUSE,
            directory.path().to_string_lossy(),
        );
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(CatalogOptions::LOCAL_CACHE_WHITELIST, "data");
        options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "4");
        options.set(CatalogOptions::LOCAL_CACHE_MAX_SIZE, "1 MiB");
        options.set(CatalogOptions::LOCAL_CACHE_SHARED, shared.to_string());
        options.set(
            CatalogOptions::LOCAL_CACHE_DIR,
            directory.path().join("cache").to_string_lossy(),
        );
        let catalog = FileSystemCatalog::new(options.clone()).unwrap();
        let cached_input = catalog.file_io().new_input(&io_path).unwrap();
        assert_eq!(cached_input.read().await.unwrap().as_ref(), b"abcdefgh");
        std::fs::write(&path, b"12345678").unwrap();
        assert_eq!(cached_input.read().await.unwrap().as_ref(), b"abcdefgh");

        options.set(CatalogOptions::LOCAL_CACHE_EXCLUDE_EXTENSIONS, "blob");
        let excluded_catalog = FileSystemCatalog::new(options).unwrap();
        let input = excluded_catalog.file_io().new_input(&io_path).unwrap();
        assert_eq!(input.read().await.unwrap().as_ref(), b"12345678");
        assert_eq!(
            input
                .reader()
                .await
                .unwrap()
                .read(1..7)
                .await
                .unwrap()
                .as_ref(),
            b"234567"
        );

        let output = excluded_catalog.file_io().new_output(&io_path).unwrap();
        output
            .write(bytes::Bytes::from_static(b"updated!"))
            .await
            .unwrap();
        assert_eq!(cached_input.read().await.unwrap().as_ref(), b"updated!");
        std::fs::write(&path, b"changed!").unwrap();
        assert_eq!(
            output.to_input_file().read().await.unwrap().as_ref(),
            b"changed!"
        );

        excluded_catalog
            .file_io()
            .delete_file(&io_path)
            .await
            .unwrap();
        assert!(matches!(
            cached_input.read().await,
            Err(Error::IoUnexpected { source, .. })
                if source.kind() == opendal::ErrorKind::NotFound
        ));
        std::fs::write(&path, b"recreate").unwrap();
        assert_eq!(cached_input.read().await.unwrap().as_ref(), b"recreate");
    }
}
