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

use super::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct PrefixProvider {
    routes: Vec<(String, Operator)>,
    calls: AtomicUsize,
    reject: AtomicBool,
}

#[async_trait::async_trait]
impl FileIOProvider for PrefixProvider {
    async fn create(&self, path: &str) -> Result<(Operator, String)> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject.load(Ordering::SeqCst) {
            return RejectingProvider.create(path).await;
        }
        for (prefix, op) in &self.routes {
            if path == prefix.trim_end_matches('/') {
                return Ok((op.clone(), String::new()));
            }
            if let Some(relative) = path.strip_prefix(prefix) {
                return Ok((op.clone(), relative.to_string()));
            }
        }
        RejectingProvider.create(path).await
    }
}

fn memory_operator() -> Operator {
    Operator::from_config(opendal::services::MemoryConfig::default()).unwrap()
}

fn provider_io(routes: Vec<(String, Operator)>) -> (FileIO, Arc<PrefixProvider>) {
    let provider = Arc::new(PrefixProvider {
        routes,
        calls: AtomicUsize::new(0),
        reject: AtomicBool::new(false),
    });
    let io = FileIOBuilder::new("application-storage")
        .with_provider(provider.clone())
        .build()
        .unwrap();
    (io, provider)
}

#[derive(Debug)]
struct RejectingProvider;

#[async_trait::async_trait]
impl FileIOProvider for RejectingProvider {
    async fn create(&self, _path: &str) -> Result<(Operator, String)> {
        Err(Error::ConfigInvalid {
            message: "provider denied access".to_string(),
        })
    }
}

#[tokio::test]
async fn provider_handles_defer_resolution_without_using_static_storage() {
    let operator = Operator::from_config(opendal::services::MemoryConfig::default()).unwrap();
    let io = FileIOBuilder::new("file")
        .with_fs_operator(operator)
        .build()
        .unwrap()
        .with_provider(Arc::new(RejectingProvider));

    let input = io.new_input("s3://bucket/key").unwrap();
    let output = io.new_output("s3://bucket/key").unwrap();
    assert_denied(input.read().await);
    assert_denied(output.write(Bytes::from_static(b"data")).await);
}

fn assert_denied<T>(result: Result<T>) {
    assert!(matches!(
        result,
        Err(Error::ConfigInvalid { message }) if message == "provider denied access"
    ));
}

#[tokio::test]
async fn provider_routes_buckets_and_preserves_literal_keys() {
    let first = memory_operator();
    let second = memory_operator();
    let (io, provider) = provider_io(vec![
        ("s3://first/".to_string(), first.clone()),
        ("oss://second/".to_string(), second.clone()),
    ]);
    let key = "table/中文 a%2Fb?part=1#fragment.parquet";
    let first_path = format!("s3://first/{key}");
    let second_path = format!("oss://second/{key}");
    let input = io.new_input(&first_path).unwrap();
    let output = io.new_output(&first_path).unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

    output
        .write(Bytes::from_static(b"first bucket"))
        .await
        .unwrap();
    io.clone()
        .new_output(&second_path)
        .unwrap()
        .write(Bytes::from_static(b"second bucket"))
        .await
        .unwrap();
    // Check the backing services directly: matching read/write mangling cannot
    // make this test pass by accidentally accessing the same wrong object key.
    assert_eq!(first.read(key).await.unwrap().to_bytes(), "first bucket");
    assert_eq!(second.read(key).await.unwrap().to_bytes(), "second bucket");
    assert_eq!(input.read().await.unwrap(), "first bucket");
    assert_eq!(
        input.reader().await.unwrap().read(1..5).await.unwrap(),
        "irst"
    );
    assert!(input.exists().await.unwrap());
    assert!(output.exists().await.unwrap());
    let meta = input.metadata().await.unwrap();
    assert_eq!(meta.path, first_path);
    assert_eq!(meta.size, 12);
    assert_eq!(io.get_status(&second_path).await.unwrap().size, 13);
    assert_eq!(output.to_input_file().read().await.unwrap(), "first bucket");

    let copied = "oss://second/table/copied";
    io.copy_file(&first_path, copied).await.unwrap();
    assert_eq!(
        second.read("table/copied").await.unwrap().to_bytes(),
        "first bucket"
    );
    io.delete_file(&first_path).await.unwrap();
    assert!(!first.exists(key).await.unwrap());
    assert!(io.exists(&second_path).await.unwrap());
    io.delete_dir("oss://second/table/").await.unwrap();
    assert!(!second.exists(key).await.unwrap());
    assert!(!second.exists("table/copied").await.unwrap());
}

#[tokio::test]
async fn provider_listings_round_trip_roots_prefixes_and_literal_keys() {
    // An operator root below the physical bucket needs a longer logical URI
    // prefix. The provider strips only this prefix; listing must restore it.
    let mut config = opendal::services::MemoryConfig::default();
    config.root = Some("/tenant/".to_string());
    let op = Operator::from_config(config).unwrap();
    let (io, _) = provider_io(vec![("s3://bucket/tenant/".to_string(), op.clone())]);
    for key in ["table/a%2Fb", "table/中文 ?#", "table/sub/c"] {
        op.write(key, key.to_string()).await.unwrap();
    }
    for dir in ["s3://bucket/tenant/table", "s3://bucket/tenant/table/"] {
        let statuses = io.list_status(dir).await.unwrap();
        assert_eq!(
            statuses
                .iter()
                .map(|s| s.path.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "s3://bucket/tenant/table/a%2Fb",
                "s3://bucket/tenant/table/中文 ?#",
                "s3://bucket/tenant/table/sub/",
            ])
        );
        for status in statuses {
            if status.is_dir {
                assert_eq!(io.list_status(&status.path).await.unwrap().len(), 1);
            } else {
                assert_eq!(
                    io.new_input(&status.path).unwrap().read().await.unwrap(),
                    status.path.strip_prefix("s3://bucket/tenant/").unwrap()
                );
            }
        }
    }
    for root in ["s3://bucket/tenant", "s3://bucket/tenant/"] {
        assert_eq!(
            io.list_status(root).await.unwrap()[0].path,
            "s3://bucket/tenant/table/"
        );
        let files = io.list_status_recursive(root).await.unwrap();
        assert_eq!(files.len(), 3);
        for file in files {
            assert!(!file.is_dir);
            assert_eq!(
                io.new_input(&file.path).unwrap().read().await.unwrap(),
                file.path.strip_prefix("s3://bucket/tenant/").unwrap()
            );
        }
    }
    let (bucket_io, _) = provider_io(vec![("s3://bucket/".to_string(), op)]);
    for root in ["s3://bucket", "s3://bucket/"] {
        assert_eq!(
            bucket_io.list_status(root).await.unwrap()[0].path,
            "s3://bucket/table/"
        );
    }
}

#[tokio::test]
async fn provider_errors_propagate_across_all_io_entrypoints() {
    let io = FileIOBuilder::new("oss")
        .with_prop("fs.oss.retry.count", "not a number")
        .with_provider(Arc::new(RejectingProvider))
        .build()
        .unwrap();
    let path = "oss://bucket/key";
    let input = io.new_input(path).unwrap();
    let output = io.new_output(path).unwrap();
    assert_denied(io.exists(path).await);
    assert_denied(io.exists_dir(path).await);
    assert_denied(io.get_status(path).await);
    assert_denied(io.list_status(path).await);
    assert_denied(io.list_status_recursive(path).await);
    assert_denied(io.mkdirs(path).await);
    assert_denied(io.delete_file(path).await);
    assert_denied(io.delete_dir(path).await);
    assert_denied(io.rename(path, "oss://bucket/target").await);
    assert_denied(io.copy_file(path, "oss://bucket/target").await);
    assert_denied(input.exists().await);
    assert_denied(input.metadata().await);
    assert_denied(input.read().await);
    assert_denied(input.reader().await);
    assert_denied(output.exists().await);
    assert_denied(output.writer().await);
    assert_denied(output.async_writer().await);
    assert_denied(output.to_input_file().read().await);
}

#[derive(Debug)]
struct FixedPathProvider {
    op: Operator,
    relative: String,
}

#[async_trait::async_trait]
impl FileIOProvider for FixedPathProvider {
    async fn create(&self, _path: &str) -> Result<(Operator, String)> {
        Ok((self.op.clone(), self.relative.clone()))
    }
}

#[tokio::test]
async fn provider_rejects_unrepresentable_keys_and_listing_mappings() {
    let op = memory_operator();
    op.write("a/b", "untouched").await.unwrap();
    let (io, _) = provider_io(vec![("s3://bucket/".to_string(), op.clone())]);
    for path in [
        "s3://bucket/a//b",
        "s3://bucket//a/b",
        "s3://bucket/a/b ",
        "s3://bucket/ a/b",
    ] {
        assert!(matches!(
            io.new_output(path)
                .unwrap()
                .write(Bytes::from_static(b"wrong"))
                .await,
            Err(Error::ConfigInvalid { .. })
        ));
        assert!(matches!(
            io.exists(path).await,
            Err(Error::ConfigInvalid { .. })
        ));
    }
    assert_eq!(op.read("a/b").await.unwrap().to_bytes(), "untouched");

    for (uri, relative) in [
        ("s3://b/a", "much/longer/than/the/original/path"),
        ("s3://b/中文", "wrong"),
        ("s3://b/foobar", "bar"),
    ] {
        let io = FileIOBuilder::new("unused")
            .with_provider(Arc::new(FixedPathProvider {
                op: op.clone(),
                relative: relative.to_string(),
            }))
            .build()
            .unwrap();
        assert!(matches!(
            io.list_status(uri).await,
            Err(Error::ConfigInvalid { .. })
        ));
        assert!(matches!(
            io.list_status_recursive(uri).await,
            Err(Error::ConfigInvalid { .. })
        ));
    }
    let root_io = FileIOBuilder::new("unused")
        .with_provider(Arc::new(FixedPathProvider {
            op,
            relative: "/".to_string(),
        }))
        .build()
        .unwrap();
    assert_eq!(
        root_io.list_status("s3://b").await.unwrap()[0].path,
        "s3://b/a/"
    );
}

#[test]
fn provider_and_fs_operator_are_mutually_exclusive() {
    for builder in [
        FileIOBuilder::new("file")
            .with_provider(Arc::new(RejectingProvider))
            .with_fs_operator(memory_operator()),
        FileIOBuilder::new("file")
            .with_fs_operator(memory_operator())
            .with_provider(Arc::new(RejectingProvider)),
    ] {
        assert!(matches!(builder.build(), Err(Error::ConfigInvalid { .. })));
    }
}

#[tokio::test]
async fn opened_reader_and_writer_keep_the_shared_backend() {
    let op = memory_operator();
    op.write("key", "before").await.unwrap();
    let (io, provider) = provider_io(vec![("s3://bucket/".to_string(), op.clone())]);
    let input = io.new_input("s3://bucket/key").unwrap();
    let reader = input.reader().await.unwrap();
    let mut writer = io
        .new_output("s3://bucket/output")
        .unwrap()
        .writer()
        .await
        .unwrap();
    assert_eq!(reader.read(0..6).await.unwrap(), "before");

    // Open handles retain the injected backend and see its shared state. They
    // cannot rely on another provider call to replace an expired credential.
    provider.reject.store(true, Ordering::SeqCst);
    let calls = provider.calls.load(Ordering::SeqCst);
    op.write("key", "after!").await.unwrap();
    assert_eq!(reader.read(0..6).await.unwrap(), "after!");
    writer.write(Bytes::from_static(b"written")).await.unwrap();
    writer.close().await.unwrap();
    assert_eq!(op.read("output").await.unwrap().to_bytes(), "written");
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
    assert_denied(input.read().await);
}

#[cfg(all(feature = "storage-fs", not(windows)))]
#[tokio::test]
async fn static_listing_preserves_literal_posix_backslashes() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join(r"literal\directory");
    std::fs::create_dir(&dir).unwrap();
    let file = dir.join("data");
    std::fs::write(&file, "data").unwrap();
    let io = FileIOBuilder::new("file").build().unwrap();
    let statuses = io
        .list_status(&format!("file:{}", dir.display()))
        .await
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].path, format!("file:{}", file.display()));
    assert_eq!(
        io.new_input(&statuses[0].path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        "data"
    );
}

fn with_memory_cache(mut io: FileIO) -> FileIO {
    use crate::io::cache::LocalCacheConfig;
    use crate::{CatalogOptions, Options};

    let mut options = Options::new();
    options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
    options.set(CatalogOptions::LOCAL_CACHE_BLOCK_SIZE, "4");
    let config = LocalCacheConfig::from_options(&options).unwrap().unwrap();
    io.cache = Some(Arc::new(LocalCache::new(config).unwrap()));
    io
}

#[tokio::test]
async fn provider_cache_isolates_buckets_and_invalidates_aliases() {
    use tokio::io::AsyncWriteExt;

    let first = memory_operator();
    let second = memory_operator();
    let (io, provider) = provider_io(vec![
        ("s3://first/".to_string(), first.clone()),
        ("s3a://first/".to_string(), first.clone()),
        ("oss://second/".to_string(), second.clone()),
    ]);
    let io = with_memory_cache(io);
    let key = "table/snapshot/snapshot-1";
    let a = io.new_input(&format!("s3://first/{key}")).unwrap();
    let b = io.new_input(&format!("oss://second/{key}")).unwrap();
    first.write(key, "aaaa").await.unwrap();
    second.write(key, "bbbb").await.unwrap();
    assert_eq!(a.read().await.unwrap(), "aaaa");
    assert_eq!(b.read().await.unwrap(), "bbbb");
    first.delete(key).await.unwrap();
    second.delete(key).await.unwrap();
    assert_eq!(a.read().await.unwrap(), "aaaa");
    assert_eq!(b.read().await.unwrap(), "bbbb");

    // Even an existing cached handle must ask its provider before serving data.
    provider.reject.store(true, Ordering::SeqCst);
    assert_denied(a.read().await);
    assert_denied(b.reader().await);
    provider.reject.store(false, Ordering::SeqCst);

    let output = io.new_output(&format!("s3a://first/{key}")).unwrap();
    output.write(Bytes::from_static(b"cccc")).await.unwrap();
    assert_eq!(a.read().await.unwrap(), "cccc");
    let mut writer = output.async_writer().await.unwrap();
    writer.write_all(b"dddd").await.unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(a.read().await.unwrap(), "dddd");

    io.delete_dir("s3a://first/table/").await.unwrap();
    assert!(a.read().await.is_err());
    io.delete_file(&format!("oss://second/{key}"))
        .await
        .unwrap();
    assert!(b.read().await.is_err());
}

#[cfg(feature = "storage-fs")]
#[tokio::test]
async fn provider_rename_uses_one_service_and_rejects_other_backends() {
    fn fs_operator(root: &std::path::Path) -> Operator {
        let mut config = opendal_service_fs::FsConfig::default();
        config.root = Some(root.to_string_lossy().to_string());
        Operator::from_config(config).unwrap()
    }
    let first_root = tempfile::tempdir().unwrap();
    let second_root = tempfile::tempdir().unwrap();
    let first = fs_operator(first_root.path());
    let second = fs_operator(second_root.path());
    let (io, _) = provider_io(vec![
        ("s3://first/".to_string(), first.clone()),
        ("oss://second/".to_string(), second.clone()),
    ]);
    let io = with_memory_cache(io);
    io.mkdirs("s3://first/table/snapshot/").await.unwrap();
    assert!(io.exists_dir("s3://first/table/snapshot").await.unwrap());
    let src = "s3://first/table/snapshot/snapshot-1";
    let dst = "s3://first/table/snapshot/snapshot-2";
    io.new_output(src)
        .unwrap()
        .write(Bytes::from_static(b"source"))
        .await
        .unwrap();
    io.new_output(dst)
        .unwrap()
        .write(Bytes::from_static(b"target"))
        .await
        .unwrap();
    assert_eq!(io.new_input(src).unwrap().read().await.unwrap(), "source");
    assert_eq!(io.new_input(dst).unwrap().read().await.unwrap(), "target");
    io.rename(src, dst).await.unwrap();
    assert!(io.new_input(src).unwrap().read().await.is_err());
    assert_eq!(io.new_input(dst).unwrap().read().await.unwrap(), "source");

    // A mistaken rename on the source operator would overwrite its local
    // `protected` file while leaving the intended destination untouched.
    first.write("protected", "local").await.unwrap();
    second.write("protected", "remote").await.unwrap();
    assert!(matches!(
        io.rename(dst, "oss://second/protected").await,
        Err(Error::IoUnsupported { .. })
    ));
    assert_eq!(first.read("protected").await.unwrap().to_bytes(), "local");
    assert_eq!(second.read("protected").await.unwrap().to_bytes(), "remote");
    assert!(io.exists(dst).await.unwrap());
}
