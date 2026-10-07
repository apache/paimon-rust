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
use crate::io::oss_test_server::TestOss;

const DATA: &str = "oss://bkt/db.db/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-1.parquet";
const DATA_KEY: &str = "bkt/db.db/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-1.parquet";
const MANIFEST: &str = "oss://bkt/db.db/t/manifest/manifest-123e4567-e89b-12d3-a456-426614174000-1";
const MANIFEST_KEY: &str = "bkt/db.db/t/manifest/manifest-123e4567-e89b-12d3-a456-426614174000-1";
const INDEX: &str = "oss://bkt/db.db/t/index/index-123e4567-e89b-12d3-a456-426614174000-1";
const LATEST: &str = "oss://bkt/db.db/t/snapshot/LATEST";
const SNAPSHOT: &str = "oss://bkt/db.db/t/snapshot/snapshot-1";
const UNREACHABLE: &str = "http://127.0.0.1:1";

fn oss_file_io(props: &[(&str, &str)]) -> FileIO {
    FileIOBuilder::new("oss")
        .with_props([
            ("fs.oss.accessKeyId", "ak"),
            ("fs.oss.accessKeySecret", "sk"),
            ("fs.oss.path-style-access", "true"),
            ("fs.oss.retry.count", "3"),
            ("fs.oss.retry.interval.millisecond", "1"),
        ])
        .with_props(props.iter().copied())
        .build()
        .unwrap()
}

/// Cache and origin servers over the same objects, and a FileIO routing to the single target.
async fn routed(policy: &str) -> (TestOss, TestOss, FileIO) {
    let origin = TestOss::start().await;
    let cache = TestOss::start_sharing(&origin).await;
    let file_io = oss_file_io(&[
        ("fs.oss.endpoint", cache.endpoint()),
        ("io-cache.enabled", "true"),
        ("io-cache.endpoint", cache.endpoint()),
        ("io-cache.target.default.path-style-access", "true"),
        ("io-cache.origin.endpoint", origin.endpoint()),
        ("io-cache.policy", policy),
    ]);
    (origin, cache, file_io)
}

/// Origin and two targets over the same objects: manifests use `accel`, data uses `cluster`.
async fn multi_target() -> (TestOss, TestOss, TestOss, FileIO) {
    let origin = TestOss::start().await;
    let accel = TestOss::start_sharing(&origin).await;
    let cluster = TestOss::start_sharing(&origin).await;
    let file_io = oss_file_io(&[
        ("fs.oss.endpoint", origin.endpoint()),
        ("io-cache.enabled", "true"),
        ("io-cache.targets", "accel,cluster"),
        ("io-cache.target.accel.endpoint", accel.endpoint()),
        ("io-cache.target.accel.path-style-access", "true"),
        ("io-cache.target.accel.region", "cn-hangzhou"),
        ("io-cache.target.cluster.endpoint", cluster.endpoint()),
        ("io-cache.target.cluster.path-style-access", "true"),
        ("io-cache.policy", "meta,read"),
        ("io-cache.routes", "meta=accel;data=cluster"),
    ]);
    (origin, accel, cluster, file_io)
}

async fn write(file_io: &FileIO, path: &str, data: &'static [u8]) {
    file_io
        .new_output(path)
        .unwrap()
        .write(Bytes::from_static(data))
        .await
        .unwrap();
}

async fn read(file_io: &FileIO, path: &str) -> Bytes {
    file_io.new_input(path).unwrap().read().await.unwrap()
}

fn is_not_found(error: crate::Error) -> bool {
    let crate::Error::IoUnexpected { source, .. } = error else {
        return false;
    };
    source.kind() == opendal::ErrorKind::NotFound
}

#[tokio::test]
async fn test_only_reads_and_status_use_the_target() {
    let (origin, cache, file_io) = routed("meta,read").await;

    write(&file_io, DATA, b"0123456789").await;
    // A writer checks existence on origin, and so does exists without the exists token.
    assert!(file_io.new_output(DATA).unwrap().exists().await.unwrap());
    assert!(file_io.exists(DATA).await.unwrap());
    let input = file_io.new_input(DATA).unwrap();
    assert!(input.exists().await.unwrap());
    assert_eq!(
        origin.take_requests(),
        [
            format!("PUT {DATA_KEY}"),
            format!("HEAD {DATA_KEY}"),
            format!("HEAD {DATA_KEY}"),
            format!("HEAD {DATA_KEY}")
        ]
    );
    assert!(cache.take_requests().is_empty());

    assert_eq!(file_io.get_status(DATA).await.unwrap().size, 10);
    assert_eq!(input.metadata().await.unwrap().size, 10);
    assert_eq!(input.read().await.unwrap(), "0123456789");
    let reader = input.reader().await.unwrap();
    assert_eq!(reader.read(2..5).await.unwrap(), "234");
    assert_eq!(
        cache.take_requests(),
        [
            format!("HEAD {DATA_KEY}"),
            format!("HEAD {DATA_KEY}"),
            format!("GET {DATA_KEY}"),
            format!("GET {DATA_KEY}"),
        ]
    );
    assert!(origin.take_requests().is_empty());

    // Listing, copies, deletes, and files that are not on the allowlist always use origin.
    let statuses = file_io
        .list_status("oss://bkt/db.db/t/bucket-0/")
        .await
        .unwrap();
    assert_eq!(statuses.len(), 1);
    write(&file_io, LATEST, b"1").await;
    assert_eq!(read(&file_io, LATEST).await, "1");
    write(&file_io, SNAPSHOT, b"{}").await;
    assert_eq!(read(&file_io, SNAPSHOT).await, "{}");
    assert_eq!(file_io.get_status(SNAPSHOT).await.unwrap().size, 2);
    let unknown = "oss://bkt/db.db/t/bucket-0/part-00000.parquet";
    write(&file_io, unknown, b"x").await;
    assert_eq!(read(&file_io, unknown).await, "x");
    file_io
        .copy_file(
            DATA,
            "oss://bkt/db.db/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-2.parquet",
        )
        .await
        .unwrap();
    file_io.delete_file(DATA).await.unwrap();
    assert!(cache.take_requests().is_empty());
    assert_eq!(
        origin.take_requests(),
        [
            "LIST bkt/db.db/t/bucket-0/".to_string(),
            "PUT bkt/db.db/t/snapshot/LATEST".to_string(),
            "GET bkt/db.db/t/snapshot/LATEST".to_string(),
            "PUT bkt/db.db/t/snapshot/snapshot-1".to_string(),
            "GET bkt/db.db/t/snapshot/snapshot-1".to_string(),
            "HEAD bkt/db.db/t/snapshot/snapshot-1".to_string(),
            "PUT bkt/db.db/t/bucket-0/part-00000.parquet".to_string(),
            "GET bkt/db.db/t/bucket-0/part-00000.parquet".to_string(),
            format!("GET {DATA_KEY}"),
            "PUT bkt/db.db/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-2.parquet"
                .to_string(),
            format!("DELETE {DATA_KEY}"),
        ]
    );
}

#[tokio::test]
async fn test_policy_selects_operation_classes() {
    let (origin, cache, file_io) = routed("read").await;

    write(&file_io, DATA, b"data").await;
    assert_eq!(file_io.get_status(DATA).await.unwrap().size, 4);
    assert_eq!(
        origin.take_requests(),
        [format!("PUT {DATA_KEY}"), format!("HEAD {DATA_KEY}")]
    );

    assert_eq!(read(&file_io, DATA).await, "data");
    assert_eq!(cache.take_requests(), [format!("GET {DATA_KEY}")]);
}

#[tokio::test]
async fn test_target_not_found_is_final() {
    let (origin, cache, file_io) = routed("meta,read").await;
    write(&file_io, DATA, b"data").await;
    origin.take_requests();
    cache.hide(DATA_KEY);

    assert!(is_not_found(file_io.get_status(DATA).await.unwrap_err()));
    let input = file_io.new_input(DATA).unwrap();
    assert!(is_not_found(input.read().await.unwrap_err()));
    let reader = input.reader().await.unwrap();
    assert!(is_not_found(reader.read(0..4).await.unwrap_err()));
    assert_eq!(
        cache.take_requests(),
        [
            format!("HEAD {DATA_KEY}"),
            format!("GET {DATA_KEY}"),
            format!("GET {DATA_KEY}")
        ]
    );
    assert!(origin.take_requests().is_empty());
}

#[tokio::test]
async fn test_target_errors_are_not_retried_on_origin() {
    let (origin, cache, file_io) = routed("meta,read").await;
    write(&file_io, DATA, b"data").await;
    origin.take_requests();
    cache.set_unavailable(true);

    assert!(file_io.get_status(DATA).await.is_err());
    assert!(file_io.new_input(DATA).unwrap().read().await.is_err());
    // The target gets the initial attempt plus fs.oss.retry.count retries; origin none.
    let served = cache.take_requests();
    let count = |method: &str| {
        served
            .iter()
            .filter(|r| **r == format!("{method} {DATA_KEY}"))
            .count()
    };
    assert_eq!((count("HEAD"), count("GET")), (4, 4));
    assert!(origin.take_requests().is_empty());
}

#[tokio::test]
async fn test_writes_use_the_target_with_write_policy() {
    let (origin, cache, file_io) = routed("meta,read,write").await;

    write(&file_io, DATA, b"data").await;
    write(&file_io, SNAPSHOT, b"{}").await;
    // A writer checks existence on origin; snapshots are never written through a cache.
    assert!(file_io.new_output(DATA).unwrap().exists().await.unwrap());
    assert_eq!(cache.take_requests(), [format!("PUT {DATA_KEY}")]);
    assert_eq!(
        origin.take_requests(),
        [
            "PUT bkt/db.db/t/snapshot/snapshot-1".to_string(),
            format!("HEAD {DATA_KEY}")
        ]
    );
    assert_eq!(origin.object(DATA_KEY).unwrap(), "data");
}

#[tokio::test]
async fn test_writes_never_use_an_unavailable_target() {
    let (origin, cache, file_io) = routed("meta,read").await;
    cache.set_unavailable(true);

    write(&file_io, DATA, b"data").await;
    let mut writer = file_io
        .new_output(MANIFEST)
        .unwrap()
        .writer()
        .await
        .unwrap();
    writer.write(Bytes::from_static(b"manifest")).await.unwrap();
    writer.close().await.unwrap();
    assert!(file_io
        .new_output(MANIFEST)
        .unwrap()
        .exists()
        .await
        .unwrap());
    assert!(cache.take_requests().is_empty());
    assert_eq!(origin.object(MANIFEST_KEY).unwrap(), "manifest");
}

#[tokio::test]
async fn test_targets_serve_the_file_types_of_their_routes() {
    let (origin, accel, cluster, file_io) = multi_target().await;
    write(&file_io, DATA, b"data").await;
    write(&file_io, MANIFEST, b"manifest").await;
    write(&file_io, INDEX, b"index").await;
    assert_eq!(origin.take_requests().len(), 3);

    assert_eq!(read(&file_io, MANIFEST).await, "manifest");
    assert_eq!(file_io.get_status(MANIFEST).await.unwrap().size, 8);
    assert_eq!(read(&file_io, DATA).await, "data");
    assert_eq!(file_io.get_status(DATA).await.unwrap().size, 4);
    // No route names bucket indexes; without the exists token existence checks use origin.
    assert_eq!(read(&file_io, INDEX).await, "index");
    assert!(file_io.exists(MANIFEST).await.unwrap());
    assert_eq!(
        accel.take_requests(),
        [
            format!("GET {MANIFEST_KEY}"),
            format!("HEAD {MANIFEST_KEY}")
        ]
    );
    assert_eq!(
        cluster.take_requests(),
        [format!("GET {DATA_KEY}"), format!("HEAD {DATA_KEY}")]
    );
    assert_eq!(
        origin.take_requests(),
        [
            "GET bkt/db.db/t/index/index-123e4567-e89b-12d3-a456-426614174000-1".to_string(),
            format!("HEAD {MANIFEST_KEY}")
        ]
    );
}

#[tokio::test]
async fn test_format_table_file_named_like_a_manifest_uses_origin() {
    let origin = TestOss::start().await;
    // Its own storage stands in for a cache that kept the file before it was replaced.
    let cache = TestOss::start().await;
    let path = "oss://bkt/db.db/review_external/manifest.parquet";
    let key = "bkt/db.db/review_external/manifest.parquet";
    write(
        &oss_file_io(&[("fs.oss.endpoint", cache.endpoint())]),
        path,
        b"id=1",
    )
    .await;
    write(
        &oss_file_io(&[("fs.oss.endpoint", origin.endpoint())]),
        path,
        b"id=2",
    )
    .await;
    cache.take_requests();
    origin.take_requests();
    let file_io = oss_file_io(&[
        ("fs.oss.endpoint", cache.endpoint()),
        ("io-cache.enabled", "true"),
        ("io-cache.endpoint", cache.endpoint()),
        ("io-cache.target.default.path-style-access", "true"),
        ("io-cache.origin.endpoint", origin.endpoint()),
        ("io-cache.policy", "meta,read"),
    ]);

    assert_eq!(read(&file_io, path).await, "id=2");
    assert_eq!(file_io.get_status(path).await.unwrap().size, 4);
    assert!(cache.take_requests().is_empty());
    assert_eq!(
        origin.take_requests(),
        [format!("GET {key}"), format!("HEAD {key}")]
    );
}

#[tokio::test]
async fn test_exists_ignores_a_cache_that_kept_a_deleted_file() {
    let origin = TestOss::start().await;
    // Its own storage stands in for a cache that kept a file after origin deleted it.
    let cache = TestOss::start().await;
    write(
        &oss_file_io(&[("fs.oss.endpoint", cache.endpoint())]),
        DATA,
        b"stale",
    )
    .await;
    let routed = |policy| {
        oss_file_io(&[
            ("fs.oss.endpoint", cache.endpoint()),
            ("io-cache.enabled", "true"),
            ("io-cache.endpoint", cache.endpoint()),
            ("io-cache.target.default.path-style-access", "true"),
            ("io-cache.origin.endpoint", origin.endpoint()),
            ("io-cache.policy", policy),
        ])
    };

    let file_io = routed("meta,read");
    assert!(!file_io.exists(DATA).await.unwrap());
    assert!(!file_io.new_input(DATA).unwrap().exists().await.unwrap());
    // With exists in the policy the target answers, so it is vended only for consistent targets.
    assert!(routed("meta,read,exists").exists(DATA).await.unwrap());
}

#[tokio::test]
async fn test_exists_uses_the_target_with_the_exists_token() {
    let (origin, cache, file_io) = routed("meta,read,exists").await;

    write(&file_io, DATA, b"data").await;
    assert!(file_io.new_output(DATA).unwrap().exists().await.unwrap());
    assert_eq!(
        origin.take_requests(),
        [format!("PUT {DATA_KEY}"), format!("HEAD {DATA_KEY}")]
    );
    assert!(file_io.exists(DATA).await.unwrap());
    assert!(file_io.new_input(DATA).unwrap().exists().await.unwrap());
    assert_eq!(
        cache.take_requests(),
        [format!("HEAD {DATA_KEY}"), format!("HEAD {DATA_KEY}")]
    );
    assert!(origin.take_requests().is_empty());
}

#[tokio::test]
async fn test_target_without_endpoint_is_skipped() {
    let origin = TestOss::start().await;
    let cluster = TestOss::start_sharing(&origin).await;
    let file_io = oss_file_io(&[
        ("fs.oss.endpoint", origin.endpoint()),
        ("io-cache.enabled", "true"),
        ("io-cache.targets", "accel,cluster"),
        ("io-cache.target.accel.path-style-access", "true"),
        ("io-cache.target.cluster.endpoint", cluster.endpoint()),
        ("io-cache.target.cluster.path-style-access", "true"),
        ("io-cache.policy", "read"),
    ]);
    write(&file_io, DATA, b"data").await;

    // Without routes, the first target with an endpoint takes every type.
    assert_eq!(read(&file_io, DATA).await, "data");
    assert_eq!(cluster.take_requests(), [format!("GET {DATA_KEY}")]);
}

#[tokio::test]
async fn test_local_cache_reads_through_the_target() {
    let (origin, cache, mut file_io) = routed("meta,read").await;
    file_io.cache = Some(Arc::new(local_cache()));
    write(&file_io, DATA, b"0123456789").await;
    origin.take_requests();

    assert_eq!(read(&file_io, DATA).await, "0123456789");
    let reader = file_io.new_input(DATA).unwrap().reader().await.unwrap();
    assert_eq!(reader.read(0..4).await.unwrap(), "0123");
    // The size and the bytes come from the target once; later reads are local.
    assert_eq!(
        cache.take_requests(),
        [format!("HEAD {DATA_KEY}"), format!("GET {DATA_KEY}")]
    );
    assert!(origin.take_requests().is_empty());
}

fn local_cache() -> crate::io::cache::LocalCache {
    use crate::io::cache::LocalCacheConfig;
    use crate::{CatalogOptions, Options};

    let mut options = Options::new();
    options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
    options.set(CatalogOptions::LOCAL_CACHE_WHITELIST, "*");
    crate::io::cache::LocalCache::new(LocalCacheConfig::from_options(&options).unwrap().unwrap())
        .unwrap()
}

#[tokio::test]
async fn test_origin_only_view_skips_the_target() {
    let (origin, cache, file_io) = routed("meta,read").await;

    let file_io = file_io.origin_only();
    write(&file_io, DATA, b"data").await;
    assert_eq!(file_io.get_status(DATA).await.unwrap().size, 4);
    assert_eq!(read(&file_io, DATA).await, "data");
    assert!(cache.take_requests().is_empty());
    assert_eq!(origin.take_requests().len(), 3);
}

#[tokio::test]
async fn test_dlf_oss_endpoint_disables_routing() {
    let target = TestOss::start().await;
    let cache = TestOss::start_sharing(&target).await;
    let file_io = oss_file_io(&[
        ("fs.oss.endpoint", cache.endpoint()),
        ("dlf.oss-endpoint", target.endpoint()),
        ("io-cache.enabled", "true"),
        ("io-cache.endpoint", cache.endpoint()),
        ("io-cache.origin.endpoint", UNREACHABLE),
        ("io-cache.policy", "meta,read"),
    ]);

    write(&file_io, DATA, b"data").await;
    assert_eq!(read(&file_io, DATA).await, "data");
    assert!(cache.take_requests().is_empty());
    assert_eq!(target.take_requests().len(), 2);
}

#[tokio::test]
async fn test_routing_is_off_unless_enabled() {
    let server = TestOss::start().await;
    let file_io = oss_file_io(&[
        ("fs.oss.endpoint", server.endpoint()),
        ("io-cache.endpoint", UNREACHABLE),
        ("io-cache.origin.endpoint", UNREACHABLE),
        ("io-cache.policy", "meta,read"),
    ]);

    let (op, _) = file_io.create_routed_static(DATA).unwrap();
    assert!(op.target().is_none());
    write(&file_io, DATA, b"data").await;
    assert_eq!(read(&file_io, DATA).await, "data");
    assert_eq!(server.take_requests().len(), 2);
}

#[tokio::test]
async fn test_cache_key_is_identical_on_target_and_origin() {
    let (_origin, _cache, file_io) = routed("read").await;

    let (op, relative_path) = file_io.create_routed_static(DATA).unwrap();
    let target = op.target().unwrap();
    let namespace = file_io.cache_namespace_for_path(DATA).unwrap();
    assert_eq!(
        cache_object_path(&namespace, target, &relative_path),
        cache_object_path(&namespace, op.origin_operator(), &relative_path)
    );
}
