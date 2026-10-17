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

//! A path-style OSS stand-in for tests that records every request it serves.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, Response, StatusCode, Uri};
use axum::Router;
use percent_encoding::percent_decode_str;

type Objects = Arc<Mutex<BTreeMap<String, Bytes>>>;

#[derive(Default)]
struct ServerState {
    objects: Objects,
    requests: Mutex<Vec<String>>,
    unavailable: AtomicBool,
    hidden: Mutex<BTreeSet<String>>,
}

pub(crate) struct TestOss {
    endpoint: String,
    state: Arc<ServerState>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestOss {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl TestOss {
    pub(crate) async fn start() -> Self {
        Self::serve(Objects::default()).await
    }

    /// A server over the same objects, like a cache in front of `origin`.
    pub(crate) async fn start_sharing(origin: &TestOss) -> Self {
        Self::serve(origin.state.objects.clone()).await
    }

    async fn serve(objects: Objects) -> Self {
        let state = Arc::new(ServerState {
            objects,
            ..ServerState::default()
        });
        let app = Router::new().fallback(handle).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            endpoint,
            state,
            server,
        }
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Answers every request with 503 while set.
    pub(crate) fn set_unavailable(&self, unavailable: bool) {
        self.state.unavailable.store(unavailable, Ordering::SeqCst);
    }

    /// Answers HEAD and GET of `bucket_and_key` with 404, like a cache that remembers it missing.
    pub(crate) fn hide(&self, bucket_and_key: &str) {
        self.state
            .hidden
            .lock()
            .unwrap()
            .insert(bucket_and_key.to_string());
    }

    /// Requests served so far, as `METHOD bucket/key`, or `LIST bucket/prefix`.
    pub(crate) fn take_requests(&self) -> Vec<String> {
        std::mem::take(&mut *self.state.requests.lock().unwrap())
    }

    pub(crate) fn object(&self, bucket_and_key: &str) -> Option<Bytes> {
        self.state
            .objects
            .lock()
            .unwrap()
            .get(bucket_and_key)
            .cloned()
    }
}

async fn handle(
    State(state): State<Arc<ServerState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let path = percent_decode_str(uri.path().trim_start_matches('/'))
        .decode_utf8_lossy()
        .into_owned();
    let query = uri.query().unwrap_or_default();
    let is_list = method == Method::GET && query.contains("list-type");
    let label = if is_list {
        let prefix = query_value(query, "prefix").unwrap_or_default();
        format!("LIST {}/{prefix}", path.trim_end_matches('/'))
    } else {
        format!("{method} {path}")
    };
    state.requests.lock().unwrap().push(label);

    if state.unavailable.load(Ordering::SeqCst) {
        return error(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable");
    }
    if is_list {
        return list(&state.objects, path.trim_end_matches('/'), query);
    }
    let mut objects = state.objects.lock().unwrap();
    if method == Method::POST && query.starts_with("delete") {
        let mut deleted = String::new();
        let body = String::from_utf8_lossy(&body);
        for key in body
            .split("<Key>")
            .skip(1)
            .filter_map(|s| s.split_once("</Key>"))
        {
            objects.remove(&format!("{}/{}", path.trim_end_matches('/'), key.0));
            deleted.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", key.0));
        }
        return response(StatusCode::OK)
            .body(Body::from(format!(
                "<DeleteResult>{deleted}</DeleteResult>"
            )))
            .unwrap();
    }
    match method {
        Method::PUT => {
            objects.insert(path, body);
            response(StatusCode::OK).body(Body::empty()).unwrap()
        }
        Method::DELETE => {
            objects.remove(&path);
            response(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        Method::HEAD | Method::GET => {
            let hidden = state.hidden.lock().unwrap().contains(&path);
            let Some(data) = objects.get(&path).cloned().filter(|_| !hidden) else {
                return error(StatusCode::NOT_FOUND, "NoSuchKey");
            };
            if method == Method::HEAD {
                return response(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, data.len())
                    .body(Body::empty())
                    .unwrap();
            }
            match headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
                Some(range) => ranged(data, range),
                None => response(StatusCode::OK).body(Body::from(data)).unwrap(),
            }
        }
        _ => error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
    }
}

fn response(status: StatusCode) -> axum::http::response::Builder {
    Response::builder()
        .status(status)
        .header(header::ETAG, "\"etag\"")
        .header(header::LAST_MODIFIED, "Wed, 30 Sep 2026 00:00:00 GMT")
}

fn error(status: StatusCode, code: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(format!(
            "<Error><Code>{code}</Code><Message>{code}</Message></Error>"
        )))
        .unwrap()
}

fn ranged(data: Bytes, range: &str) -> Response<Body> {
    let (start, end) = range
        .trim_start_matches("bytes=")
        .split_once('-')
        .unwrap_or_default();
    let start: usize = start.parse().unwrap_or(0);
    let end: usize = end
        .parse::<usize>()
        .map_or(data.len(), |end| (end + 1).min(data.len()));
    if start >= end {
        return error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange");
    }
    response(StatusCode::PARTIAL_CONTENT)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{}/{}", end - 1, data.len()),
        )
        .body(Body::from(data.slice(start..end)))
        .unwrap()
}

fn query_value(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| percent_decode_str(v).decode_utf8_lossy().into_owned())
    })
}

/// ListObjectsV2 over one bucket; a single page is enough for tests.
fn list(objects: &Objects, bucket: &str, query: &str) -> Response<Body> {
    let prefix = query_value(query, "prefix").unwrap_or_default();
    let delimiter = query_value(query, "delimiter").unwrap_or_default();
    let bucket_prefix = format!("{bucket}/");
    let mut contents = String::new();
    let mut common_prefixes = BTreeSet::new();
    for (path, data) in objects.lock().unwrap().iter() {
        let Some(key) = path.strip_prefix(&bucket_prefix) else {
            continue;
        };
        let Some(rest) = key.strip_prefix(&prefix) else {
            continue;
        };
        match rest
            .find(delimiter.as_str())
            .filter(|_| !delimiter.is_empty())
        {
            Some(index) => {
                common_prefixes.insert(format!("{prefix}{}", &rest[..=index]));
            }
            None => contents.push_str(&format!(
                "<Contents><Key>{key}</Key><LastModified>2026-09-30T00:00:00.000Z\
                 </LastModified><ETag>\"etag\"</ETag><Size>{}</Size></Contents>",
                data.len()
            )),
        }
    }
    let common_prefixes: String = common_prefixes
        .into_iter()
        .map(|prefix| format!("<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>"))
        .collect();
    response(StatusCode::OK)
        .body(Body::from(format!(
            "<ListBucketResult><Prefix>{prefix}</Prefix><IsTruncated>false</IsTruncated>\
             {contents}{common_prefixes}</ListBucketResult>"
        )))
        .unwrap()
}
