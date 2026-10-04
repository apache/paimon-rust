// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::io::FileRead;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Method, Response, StatusCode, Uri};
use axum::Router;

fn props() -> HashMap<String, String> {
    [
        ("fs.oss.impl", "cpp"),
        ("fs.oss.cpp.library.path", "/not/a/real/library.so"),
        ("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com"),
        ("fs.oss.region", "cn-hangzhou"),
        ("fs.oss.accessKeyId", "test-ak"),
        ("fs.oss.accessKeySecret", "test-secret"),
        ("fs.oss.securityToken", "test-sts"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[test]
fn config_defaults_validation_and_redaction() {
    let config = oss_cpp_config_parse(props()).unwrap();
    assert_eq!(config.concurrency, 8);
    assert_eq!(config.retry_attempts, 3);
    assert_eq!(
        config.strings[0].to_str().unwrap(),
        "https://oss-cn-hangzhou.aliyuncs.com"
    );
    let debug = format!("{config:?}");
    assert!(!debug.contains("test-secret"));
    assert!(!debug.contains("test-sts"));
    for (key, value) in [
        ("fs.oss.cpp.max.concurrent.requests", "0"),
        ("fs.oss.cpp.max.concurrent.requests", "-1"),
        ("fs.oss.cpp.request.timeout-ms", "bad"),
        ("fs.oss.cpp.retry.max-attempts", "0"),
        ("fs.oss.cpp.path-style", "yes"),
        ("fs.oss.endpoint", "ftp://host"),
        ("fs.oss.endpoint", "https://secret@host"),
        ("fs.oss.endpoint", "https://host/path"),
        ("fs.oss.accessKeySecret", "nul\0secret"),
    ] {
        let mut options = props();
        options.insert(key.to_string(), value.to_string());
        assert!(oss_cpp_config_parse(options).is_err(), "{key}");
    }
    for key in [
        "fs.oss.region",
        "fs.oss.endpoint",
        "fs.oss.cpp.library.path",
    ] {
        let mut options = props();
        options.remove(key);
        assert!(oss_cpp_config_parse(options).is_err(), "{key}");
    }
}

#[tokio::test]
async fn selection_is_explicit_and_missing_library_fails() {
    let file_io = crate::io::FileIOBuilder::new("oss")
        .with_props(props())
        .build()
        .unwrap();
    assert!(file_io.get_status("oss://bucket/object").await.is_err());
    let config = oss_cpp_config_parse(props()).unwrap();
    let op = oss_cpp_config_build(&config, "bucket").unwrap();
    assert_eq!(op.info().scheme(), "oss-cpp");
    assert_eq!(
        op.write("object", "data").await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        op.delete("object").await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
}

#[test]
fn malformed_metadata_and_error_mapping() {
    assert!(metadata(-1, "", false).is_err());
    assert!(metadata(1, "broken", false).is_err());
    assert!(metadata(1, "Wed, 01 Jan 2025 00:00:00 GMT", false)
        .unwrap()
        .last_modified()
        .is_some());
    assert!(metadata(1, "2025-01-01T00:00:00Z", false)
        .unwrap()
        .last_modified()
        .is_some());
    for (status, kind) in [
        (404, ErrorKind::NotFound),
        (403, ErrorKind::PermissionDenied),
        (503, ErrorKind::RateLimited),
        (429, ErrorKind::RateLimited),
        (416, ErrorKind::RangeNotSatisfied),
    ] {
        let error = NativeError {
            status,
            ..Default::default()
        };
        assert_eq!(error.check(-1).unwrap_err().kind(), kind);
    }
}

#[derive(Clone, Default)]
struct Mock {
    retries: Arc<AtomicUsize>,
    partial_attempts: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    heads: Arc<AtomicUsize>,
    tokens: Arc<Mutex<Vec<String>>>,
    ranges: Arc<Mutex<Vec<String>>>,
    list_requests: Arc<Mutex<Vec<(String, String)>>>,
    user_agents: Arc<Mutex<Vec<String>>>,
}

async fn mock(
    State(state): State<Mock>,
    method: Method,
    uri: Uri,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response<Body> {
    if let Some(token) = headers.get("x-oss-security-token") {
        state
            .tokens
            .lock()
            .unwrap()
            .push(token.to_str().unwrap().to_string());
    }
    if let Some(ua) = headers.get("user-agent") {
        state
            .user_agents
            .lock()
            .unwrap()
            .push(ua.to_str().unwrap().to_string());
    }
    let path = uri.path();
    let status = if path.ends_with("missing") {
        404
    } else if path.ends_with("denied") {
        403
    } else if path.ends_with("throttle")
        || (path.ends_with("retry") && state.retries.fetch_add(1, Ordering::SeqCst) == 0)
    {
        503
    } else {
        200
    };
    if status != 200 {
        let code = match status {
            404 => "NoSuchKey",
            403 => "AccessDenied",
            _ => "SlowDown",
        };
        return Response::builder().status(status).header("x-oss-request-id", "test-request")
            .body(Body::from(format!("<Error><Code>{code}</Code><Message>test</Message><RequestId>test-request</RequestId></Error>"))).unwrap();
    }
    if query.contains_key("list-type") {
        let prefix = query.get("prefix").map(String::as_str).unwrap_or("");
        state.list_requests.lock().unwrap().push((
            prefix.to_string(),
            query.get("max-keys").cloned().unwrap_or_default(),
        ));
        if prefix == "denied/" {
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::from(
                    "<Error><Code>AccessDenied</Code><Message>test</Message></Error>",
                ))
                .unwrap();
        }
        if prefix == "missing/" {
            return Response::new(Body::from(
                "<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>",
            ));
        }
        let page = if prefix == "bad/" {
            "<IsTruncated>true</IsTruncated>".to_string()
        } else if query.contains_key("continuation-token") {
            assert_eq!(query["continuation-token"], "page2");
            "<IsTruncated>false</IsTruncated><Contents><Key>objects/b</Key><Size>10</Size><LastModified>2025-01-01T00:00:00Z</LastModified></Contents>".to_string()
        } else {
            let common = if query.contains_key("delimiter") {
                "<CommonPrefixes><Prefix>objects/sub/</Prefix></CommonPrefixes>"
            } else {
                ""
            };
            format!("<IsTruncated>true</IsTruncated><NextContinuationToken>page2</NextContinuationToken><Contents><Key>objects/a</Key><Size>10</Size><LastModified>2025-01-01T00:00:00Z</LastModified></Contents>{common}")
        };
        return Response::new(Body::from(format!(
            "<ListBucketResult><Name>bucket</Name>{page}</ListBucketResult>"
        )));
    }
    if method == Method::HEAD {
        state.heads.fetch_add(1, Ordering::SeqCst);
        return Response::builder()
            .header("content-length", "10")
            .header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
            .body(Body::empty())
            .unwrap();
    }
    if path.ends_with("slow") {
        let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
        state.peak.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        state.active.fetch_sub(1, Ordering::SeqCst);
    }
    let range = headers.get("range").unwrap().to_str().unwrap().to_string();
    if path.ends_with("partial") && state.partial_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
        let chunks = futures::stream::iter([
            Ok(bytes::Bytes::from_static(b"xx")),
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "test body disconnect",
            )),
        ]);
        return Response::builder()
            .status(206)
            .header("content-range", "bytes 2-5/10")
            .header("content-length", "4")
            .body(Body::from_stream(chunks))
            .unwrap();
    }
    state.ranges.lock().unwrap().push(range.clone());
    let (start, end) = range.trim_start_matches("bytes=").split_once('-').unwrap();
    let start: usize = start.parse().unwrap();
    let end: usize = end.parse().unwrap();
    if end >= 10 || start > end {
        return Response::builder().status(416).body(Body::empty()).unwrap();
    }
    let content_range = if path.ends_with("wrong") {
        "bytes 0-3/10".to_string()
    } else {
        format!("bytes {start}-{end}/10")
    };
    let body = if path.ends_with("short") {
        "x".as_bytes()
    } else {
        &b"0123456789"[start..=end]
    };
    Response::builder()
        .status(if path.ends_with("ignored") {
            StatusCode::OK
        } else {
            StatusCode::PARTIAL_CONTENT
        })
        .header("content-range", content_range)
        .body(Body::from(body))
        .unwrap()
}

async fn fixture(
    concurrency: usize,
) -> (
    Operator,
    Mock,
    tokio::task::JoinHandle<()>,
    HashMap<String, String>,
) {
    let library = std::env::var("OSS_CPP_BRIDGE_LIBRARY")
        .expect("Set OSS_CPP_BRIDGE_LIBRARY to the real compiled bridge");
    let state = Mock::default();
    let app = Router::new().fallback(mock).with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut options = props();
    options.insert("fs.oss.endpoint".into(), format!("http://{address}"));
    options.insert("fs.oss.cpp.library.path".into(), library);
    options.insert("fs.oss.cpp.path-style".into(), "true".into());
    options.insert(
        "fs.oss.cpp.max.concurrent.requests".into(),
        concurrency.to_string(),
    );
    options.insert("fs.oss.cpp.retry.max-attempts".into(), "2".into());
    let config = oss_cpp_config_parse(options.clone()).unwrap();
    (
        oss_cpp_config_build(&config, "bucket").unwrap(),
        state,
        server,
        options,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the compiled OSS C++ SDK bridge; no cloud credentials"]
async fn real_sdk_reads_errors_pagination_and_credentials() {
    let (op, state, server, mut options) = fixture(2).await;
    assert!(op.stat("").await.unwrap().is_dir());
    assert!(op.stat("objects/").await.unwrap().is_dir());
    assert!(!op.exists("missing/").await.unwrap());
    assert_eq!(
        op.stat("denied/").await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    let file_io = crate::io::FileIOBuilder::new("oss")
        .with_props(options.clone())
        .build()
        .unwrap();
    assert!(file_io.exists_dir("oss://bucket/objects").await.unwrap());
    assert!(!file_io.exists_dir("oss://bucket/missing").await.unwrap());
    assert!(file_io.exists_dir("oss://bucket/denied").await.is_err());
    {
        let list_requests = state.list_requests.lock().unwrap();
        assert!(
            list_requests.contains(&("objects/".to_string(), "1".to_string())),
            "list requests: {list_requests:?}"
        );
    }
    assert_eq!(op.stat("data").await.unwrap().content_length(), 10);
    let before = state.heads.load(Ordering::SeqCst);
    let reader = op.reader("data").await.unwrap();
    assert_eq!(reader.read(2..6).await.unwrap().to_bytes(), &b"2345"[..]);
    assert_eq!(
        state.heads.load(Ordering::SeqCst),
        before,
        "bounded range must not HEAD"
    );
    assert_eq!(
        op.read("data").await.unwrap().to_bytes(),
        &b"0123456789"[..]
    );
    assert_eq!(reader.read(0..0).await.unwrap().len(), 0);
    for key in ["short", "wrong", "ignored"] {
        assert!(
            op.reader(key).await.unwrap().read(2..6).await.is_err(),
            "{key}"
        );
    }
    assert!(!op.exists("missing").await.unwrap());
    assert_eq!(
        op.stat("denied").await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert_eq!(
        op.reader("retry")
            .await
            .unwrap()
            .read(2..6)
            .await
            .unwrap()
            .to_bytes(),
        &b"2345"[..]
    );
    assert_eq!(state.retries.load(Ordering::SeqCst), 2);
    assert_eq!(
        op.reader("partial")
            .await
            .unwrap()
            .read(2..6)
            .await
            .unwrap()
            .to_bytes(),
        &b"2345"[..]
    );
    assert_eq!(state.partial_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        reader
            .read(BytesRange::Suffix { size: 3 })
            .await
            .unwrap()
            .to_bytes(),
        &b"789"[..]
    );
    assert!(reader.read(11..).await.is_err());
    assert_eq!(
        op.reader("throttle")
            .await
            .unwrap()
            .read(2..6)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::RateLimited
    );
    let entries = op.list("objects/").await.unwrap();
    assert_eq!(
        entries.iter().map(|e| e.path()).collect::<Vec<_>>(),
        ["objects/a", "objects/sub/", "objects/b"]
    );
    assert_eq!(
        op.list_with("objects/")
            .recursive(true)
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(op.list("bad/").await.is_err());
    assert!(state
        .user_agents
        .lock()
        .unwrap()
        .iter()
        .all(|ua| ua.contains("oss-cpp")));
    assert!(state
        .tokens
        .lock()
        .unwrap()
        .iter()
        .all(|token| token == "test-sts"));
    // Token refresh creates a new FileIO; do not share a client across credentials.
    options.insert("fs.oss.securityToken".into(), "refreshed-sts".into());
    let refreshed = crate::io::FileIOBuilder::new("oss")
        .with_props(options)
        .build()
        .unwrap();
    assert_eq!(
        refreshed
            .get_status("oss://bucket/data")
            .await
            .unwrap()
            .size,
        10
    );
    assert_eq!(
        state.tokens.lock().unwrap().last().unwrap(),
        "refreshed-sts"
    );
    // Existing readers retain the original client's lifetime and credentials.
    assert_eq!(reader.read(2..6).await.unwrap().to_bytes(), &b"2345"[..]);
    assert_eq!(state.tokens.lock().unwrap().last().unwrap(), "test-sts");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the compiled OSS C++ SDK bridge; no cloud credentials"]
async fn real_sdk_request_gate_and_cancelled_future() {
    let (op, state, server, _) = fixture(2).await;
    let reads = (0..8).map(|_| {
        let op = op.clone();
        async move {
            op.reader("slow").await.unwrap().read(0..4).await.unwrap();
        }
    });
    futures::future::join_all(reads).await;
    assert_eq!(state.peak.load(Ordering::SeqCst), 2);
    server.abort();

    let (_, state, server, options) = fixture(1).await;
    let file_io = crate::io::FileIOBuilder::new("oss")
        .with_props(options)
        .build()
        .unwrap();
    let reads = (0..8).map(|i| {
        let file_io = file_io.clone();
        async move {
            let bucket = if i % 2 == 0 { "bucket-a" } else { "bucket-b" };
            let path = format!("oss://{bucket}/slow");
            let reader = file_io.new_input(&path).unwrap().reader().await.unwrap();
            assert_eq!(reader.read(0..4).await.unwrap(), &b"0123"[..]);
        }
    });
    futures::future::join_all(reads).await;
    assert_eq!(
        state.peak.load(Ordering::SeqCst),
        1,
        "two buckets in one FileIO must share the request gate"
    );
    server.abort();

    let (op, state, server, _) = fixture(1).await;
    let first = op.clone();
    let task = tokio::spawn(async move { first.reader("slow").await.unwrap().read(0..4).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.active.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    op.reader("slow").await.unwrap().read(0..4).await.unwrap();
    assert_eq!(
        state.peak.load(Ordering::SeqCst),
        1,
        "cancelled caller must retain permit until IO completes"
    );
    server.abort();
}
