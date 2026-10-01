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

mod common;

use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray, RecordBatch};
use axum::{http::StatusCode, response::Redirect, routing::get, Router};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{BlobDescriptor, BlobType, DataType, IntType, Schema, TableSchema};
use paimon::table::BlobReader;
use std::sync::Arc;

fn assert_redacted(error: &paimon::Error) {
    let mut rendered = format!("{error}\n{error:?}");
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        rendered.push_str(&format!("\n{cause}\n{cause:?}"));
        source = cause.source();
    }
    for secret in [
        "original-user",
        "original-password",
        "original-token",
        "original-fragment",
        "redirect-secret",
    ] {
        assert!(
            !rendered.contains(secret),
            "HTTP credential leaked: {rendered}"
        );
    }
}

#[tokio::test]
async fn http_errors_redact_direct_and_redirected_credentials_in_public_blob_apis() {
    let app = Router::new()
        .route(
            "/redirect",
            get(|| async { Redirect::temporary("/failure?signature=redirect-secret") }),
        )
        .route("/failure", get(|| async { StatusCode::FORBIDDEN }))
        .route("/short", get(|| async { "x" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for route in ["failure", "redirect", "short"] {
        for length in [-1, 2] {
            // A one-byte resource only fails a bounded read, not length=-1.
            if route == "short" && length == -1 {
                continue;
            }
            for inline in [false, true] {
                let mut schema = Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .column("payload", DataType::Blob(BlobType::new()))
                    .option("row-tracking.enabled", "true")
                    .option("data-evolution.enabled", "true");
                if inline {
                    schema = schema.option("blob-descriptor-field", "payload");
                }
                let path = "memory:/http_failure";
                let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
                setup_dirs(&io, path).await;
                persist_table_schema(&io, path, table.schema()).await;
                let uri = if route == "redirect" {
                    // Credentials exist only on the redirected URL.
                    format!("http://{address}/redirect")
                } else {
                    format!("http://original-user:original-password@{address}/{route}?signature=original-token#original-fragment")
                };
                let descriptor = BlobDescriptor::new(uri, 0, length).serialize();
                let error = BlobReader::from_file_io(io.clone())
                    .read_blobs(std::slice::from_ref(&descriptor))
                    .await
                    .unwrap_err();
                assert_redacted(&error);
                if route != "short" {
                    assert!(error.to_string().contains("403"));
                }
                let input = RecordBatch::try_from_iter([
                    ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
                    (
                        "payload",
                        Arc::new(LargeBinaryArray::from(vec![Some(descriptor.as_slice())]))
                            as ArrayRef,
                    ),
                ])
                .unwrap();
                let builder = table.new_write_builder();
                let mut writer = builder.new_write().unwrap();
                let error = if inline {
                    writer.write_arrow_batch(&input).await.unwrap();
                    builder
                        .new_commit()
                        .commit(writer.prepare_commit().await.unwrap())
                        .await
                        .unwrap();
                    let read = table.new_read_builder();
                    let plan = read.new_scan().plan().await.unwrap();
                    read.new_read()
                        .unwrap()
                        .to_arrow(plan.splits())
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap_err()
                } else {
                    writer.write_arrow_batch(&input).await.unwrap_err()
                };
                assert_redacted(&error);
                if route != "short" {
                    assert!(error.to_string().contains("403"));
                }
            }
        }
    }
    server.abort();
}
