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

use super::{LocalCache, LocalCacheConfig};
use crate::common::{CatalogOptions, Options};
use crate::io::{FileIO, FileIOBuilder};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;

fn shared_io(root: &Path) -> FileIO {
    let mut options = Options::new();
    options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
    options.set("local-cache.shared", "true");
    options.set("local-cache.max-size", "16 mb");
    options.set(
        CatalogOptions::LOCAL_CACHE_DIR,
        root.join("cache").to_string_lossy(),
    );
    options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "4");
    let cache =
        LocalCache::new(LocalCacheConfig::from_options(&options).unwrap().unwrap()).unwrap();
    FileIOBuilder::new("file")
        .with_local_cache(Arc::new(cache))
        .build()
        .unwrap()
}

fn file_path(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    if normalized.starts_with('/') {
        format!("file:{normalized}")
    } else {
        format!("file:/{normalized}")
    }
}

#[tokio::test]
async fn short_first_block_is_a_complete_file() {
    let root = tempfile::tempdir().unwrap();
    let io = shared_io(root.path());
    let path = root.path().join("snapshot-1");
    std::fs::write(&path, b"abc").unwrap();
    let input = io.new_input(&file_path(&path)).unwrap();
    assert_eq!(input.read().await.unwrap().as_ref(), b"abc");
    std::fs::rename(&path, root.path().join("offline")).unwrap();
    // The cached complete file must not require a source stat.
    assert_eq!(input.read().await.unwrap().as_ref(), b"abc");
}

#[tokio::test]
async fn full_first_block_does_not_imply_eof() {
    let root = tempfile::tempdir().unwrap();
    let io = shared_io(root.path());
    for data in [&b"abcd"[..], &b"abcdef"[..]] {
        let path = root.path().join(format!("snapshot-{}", data.len()));
        std::fs::write(&path, data).unwrap();
        let input = io.new_input(&file_path(&path)).unwrap();
        assert_eq!(input.read().await.unwrap().as_ref(), data);
        std::fs::remove_file(&path).unwrap();
        assert!(input.read().await.is_err());
    }
}

#[tokio::test]
async fn replacement_child() {
    let Some(root) = std::env::var_os("PAIMON_SMALL_FILE_TEST_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    shared_io(root)
        .new_output(&file_path(&root.join("snapshot-1")))
        .unwrap()
        .write(Bytes::from_static(b"replacement"))
        .await
        .unwrap();
}

#[tokio::test]
async fn another_process_invalidates_the_small_file() {
    let root = tempfile::tempdir().unwrap();
    let io = shared_io(root.path());
    let path = root.path().join("snapshot-1");
    std::fs::write(&path, b"abc").unwrap();
    let input = io.new_input(&file_path(&path)).unwrap();
    assert_eq!(input.read().await.unwrap().as_ref(), b"abc");
    assert!(std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "io::cache::small_file_tests::replacement_child",
            "--nocapture"
        ])
        .env("PAIMON_SMALL_FILE_TEST_ROOT", root.path())
        .status()
        .unwrap()
        .success());
    assert_eq!(input.read().await.unwrap().as_ref(), b"replacement");
}
