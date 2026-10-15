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

//! URI dispatch for Blob references, matching Java's UriReaderFactory.
//! Ordinary paths retain the table's FileIO, including provider credentials.

use super::{FileIO, FileRead, InputFile};
use crate::{Error, Result};
use bytes::{Buf, Bytes, BytesMut};
use reqwest::header::CONTENT_ENCODING;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

mod custom;
pub(crate) use custom::ReusingBlobRefStreamProvider;
pub use custom::{UriInputStream, UriReader, UriReaderFactory};

pub(crate) enum UriInput {
    File(InputFile),
    Http(HttpReader),
}

impl UriInput {
    pub(crate) fn new(file_io: &FileIO, uri: &str) -> Result<Self> {
        let http = uri.split_once(':').is_some_and(|(scheme, _)| {
            scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
        });
        if http {
            Ok(Self::Http(HttpReader { uri: uri.into() }))
        } else {
            Ok(Self::File(file_io.new_input(uri)?))
        }
    }

    pub(crate) async fn size(&self) -> Result<u64> {
        match self {
            Self::File(input) => Ok(input.metadata().await?.size),
            Self::Http(reader) => reader.size().await,
        }
    }

    pub(crate) async fn reader(&self) -> Result<Arc<dyn FileRead>> {
        match self {
            Self::File(input) => Ok(Arc::new(input.reader().await?)),
            Self::Http(reader) => Ok(Arc::new(reader.clone())),
        }
    }

    /// Reuse one HTTP response while copying a descriptor into a Blob file.
    /// Reopening a Range-ignoring endpoint per chunk would reread every prefix.
    pub(crate) async fn reader_for_range(&self, range: Range<u64>) -> Result<Arc<dyn FileRead>> {
        match self {
            Self::File(input) => Ok(Arc::new(input.reader().await?)),
            Self::Http(reader) => Ok(Arc::new(reader.open(range).await?)),
        }
    }
}

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

#[derive(Clone)]
pub(crate) struct HttpReader {
    uri: String,
}

impl HttpReader {
    fn request(&self) -> reqwest::RequestBuilder {
        HTTP_CLIENT.get(&self.uri)
    }

    async fn response(&self) -> Result<reqwest::Response> {
        let response = self
            .request()
            .send()
            .await
            .map_err(http_error)?
            .error_for_status()
            .map_err(http_error)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(invalid(&format!(
                "Unexpected HTTP Blob status: {}",
                response.status()
            )));
        }
        // Reqwest removes Content-Encoding after decoding gzip/deflate.
        // Never silently copy an unsupported encoded representation as payload.
        if response
            .headers()
            .get(CONTENT_ENCODING)
            .is_some_and(|value| value.as_bytes() != b"identity")
        {
            return Err(invalid("Unsupported HTTP Blob Content-Encoding"));
        }
        Ok(response)
    }

    async fn size(&self) -> Result<u64> {
        // GET also works with endpoints that reject HEAD. A chunked or decoded
        // response has no known size: count without retaining the payload.
        let mut response = self.response().await?;
        if let Some(size) = response.content_length() {
            return Ok(size);
        }
        let mut size = 0_u64;
        while let Some(chunk) = response.chunk().await.map_err(http_error)? {
            size = size
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| invalid("HTTP Blob size exceeds u64"))?;
        }
        Ok(size)
    }
}

impl HttpReader {
    async fn open(&self, range: Range<u64>) -> Result<HttpRangeReader> {
        if range.start >= range.end {
            return Err(invalid("Invalid HTTP Blob range"));
        }
        // Like Java's HttpUriReader, offsets address the decoded entity.
        // A wire Range may instead address compressed bytes, so use a decoded
        // GET stream and skip the prefix once for the entire copy.
        let response = self.response().await?;
        let skip = range.start;
        Ok(HttpRangeReader {
            end: range.end,
            state: tokio::sync::Mutex::new(HttpRangeState {
                response,
                position: range.start,
                skip,
                pending: Bytes::new(),
            }),
        })
    }
}

#[async_trait::async_trait]
impl FileRead for HttpReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        self.open(range.clone()).await?.read(range).await
    }
}

struct HttpRangeReader {
    end: u64,
    state: tokio::sync::Mutex<HttpRangeState>,
}

struct HttpRangeState {
    response: reqwest::Response,
    position: u64,
    skip: u64,
    pending: Bytes,
}

#[async_trait::async_trait]
impl FileRead for HttpRangeReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let mut state = self.state.lock().await;
        if range.start != state.position || range.end < range.start || range.end > self.end {
            return Err(invalid("HTTP Blob copy requires consecutive ranges"));
        }
        let length = range.end - range.start;
        let mut remaining = length;
        let mut result = BytesMut::new();
        while remaining > 0 {
            if state.pending.is_empty() {
                let Some(chunk) = state.response.chunk().await.map_err(http_error)? else {
                    return Err(invalid(&format!(
                        "HTTP Blob short read: expected {length} bytes, received {}",
                        length - remaining
                    )));
                };
                state.pending = chunk;
            }
            let skip = state.skip.min(state.pending.len() as u64) as usize;
            state.skip -= skip as u64;
            state.pending.advance(skip);
            let count = remaining.min(state.pending.len() as u64) as usize;
            result.extend_from_slice(&state.pending[..count]);
            state.pending.advance(count);
            remaining -= count as u64;
        }
        state.position = range.end;
        Ok(result.freeze())
    }
}

fn http_error(error: reqwest::Error) -> Error {
    // Both Display and Debug/source chains can expose the final URL after a
    // redirect. Like Java HttpClientUtils, retain safe diagnostics only; even
    // removing the request URL would not sanitize arbitrary nested causes.
    let message = if let Some(status) = error.status() {
        format!("HTTP Blob request failed with status {status}")
    } else {
        let operation = if error.is_timeout() {
            "timeout"
        } else if error.is_redirect() {
            "redirect"
        } else if error.is_connect() {
            "connection"
        } else if error.is_decode() {
            "response decoding"
        } else if error.is_builder() {
            "request construction"
        } else if error.is_body() {
            "response body"
        } else {
            "request"
        };
        format!("HTTP Blob {operation} failed")
    };
    Error::UnexpectedError {
        message,
        source: None,
    }
}

/// Safe URI context for Blob I/O errors, including signed URLs and userinfo.
pub(crate) fn sanitize_blob_uri(uri: &str) -> String {
    if let Ok(mut url) = url::Url::parse(uri) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        return url.to_string();
    }
    if uri.split_once(':').is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    }) {
        // A malformed authority cannot be safely split into host/userinfo.
        return "<invalid HTTP URI>".into();
    }
    uri.split(['?', '#']).next().unwrap_or(uri).to_string()
}

fn invalid(message: &str) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Response, StatusCode};
    use axum::response::Redirect;
    use axum::routing::get;
    use axum::Router;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn sanitize_http_uri_context() {
        assert_eq!(
            sanitize_blob_uri("https://user:password@example.com/path?signature=secret#fragment"),
            "https://example.com/path"
        );
        assert_eq!(
            sanitize_blob_uri("http://user:password@example.com:bad/path?signature=secret"),
            "<invalid HTTP URI>"
        );
    }

    #[tokio::test]
    async fn http_reads_use_decoded_offsets_and_reuse_the_copy_stream() {
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let app = Router::new()
            .route(
                "/copy",
                get(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    async { "0123456789" }
                }),
            )
            .route("/ignored", get(|| async { "0123456789" }))
            .route(
                "/gzip",
                get(|| async {
                    let mut encoder =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(b"0123456789").unwrap();
                    Response::builder()
                        .header(CONTENT_ENCODING, "gzip")
                        .body(Body::from(encoder.finish().unwrap()))
                        .unwrap()
                }),
            )
            .route(
                "/deflate",
                get(|| async {
                    let mut encoder =
                        flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(b"0123456789").unwrap();
                    Response::builder()
                        .header(CONTENT_ENCODING, "deflate")
                        .body(Body::from(encoder.finish().unwrap()))
                        .unwrap()
                }),
            )
            .route("/no-content", get(|| async { StatusCode::NO_CONTENT }))
            .route(
                "/redirect",
                get(|| async { Redirect::temporary("/ignored") }),
            )
            .route(
                "/chunked",
                get(|| async {
                    Body::from_stream(futures::stream::iter([
                        Ok::<_, std::io::Error>(Bytes::from_static(b"012")),
                        Ok(Bytes::from_static(b"34567")),
                        Ok(Bytes::from_static(b"89")),
                    ]))
                }),
            )
            .route(
                "/encoded",
                get(|| async {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_ENCODING, "unknown")
                        .body(Body::from("0123"))
                        .unwrap()
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        for route in ["ignored", "redirect", "chunked", "gzip", "deflate"] {
            let input = UriInput::new(&io, &format!("http://{address}/{route}")).unwrap();
            let reader = input.reader().await.unwrap();
            assert_eq!(
                reader.read(3..7).await.unwrap().as_ref(),
                b"3456",
                "{route}"
            );
            assert!(reader.read(4..4).await.unwrap().is_empty());
            assert_eq!(input.size().await.unwrap(), 10, "{route}");
        }
        let ignored = UriInput::new(&io, &format!("http://{address}/ignored")).unwrap();
        assert!(ignored
            .reader()
            .await
            .unwrap()
            .read(8..12)
            .await
            .unwrap_err()
            .to_string()
            .contains("short read"));
        let copy = UriInput::new(&io, &format!("http://{address}/copy")).unwrap();
        let reader = copy.reader_for_range(1..9).await.unwrap();
        assert_eq!(reader.read(1..4).await.unwrap().as_ref(), b"123");
        assert_eq!(reader.read(4..9).await.unwrap().as_ref(), b"45678");
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        let empty_status = UriInput::new(&io, &format!("http://{address}/no-content")).unwrap();
        assert!(empty_status
            .size()
            .await
            .unwrap_err()
            .to_string()
            .contains("204"));
        let encoded = UriInput::new(&io, &format!("http://{address}/encoded")).unwrap();
        assert!(encoded
            .reader()
            .await
            .unwrap()
            .read(0..2)
            .await
            .unwrap_err()
            .to_string()
            .contains("Content-Encoding"));
        let missing = UriInput::new(&io, &format!("http://{address}/missing")).unwrap();
        assert!(missing
            .size()
            .await
            .unwrap_err()
            .to_string()
            .contains("404"));
        assert!(missing
            .reader()
            .await
            .unwrap()
            .read(0..1)
            .await
            .unwrap_err()
            .to_string()
            .contains("404"));
        server.abort();
    }
}
