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

#![cfg(any(feature = "storage-fs", feature = "storage-s3"))]

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use opendal::Operator;
use paimon::io::FileIOProvider;
use paimon::{
    catalog::Identifier,
    io::FileIOBuilder,
    spec::{DataType, IntType, Schema, TableSchema},
    table::Table,
};
use std::sync::{Arc, Mutex};

#[cfg(feature = "storage-fs")]
#[derive(Debug)]
struct StrictParentProvider {
    operator: Operator,
    parent_resolutions: Mutex<Vec<String>>,
}

#[cfg(feature = "storage-fs")]
#[async_trait::async_trait]
impl FileIOProvider for StrictParentProvider {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        let relative = path.strip_prefix("s3://bucket/").unwrap();
        if relative.ends_with('/') || relative.ends_with("/consumer") {
            self.parent_resolutions
                .lock()
                .unwrap()
                .push(relative.trim_end_matches('/').into());
        } else if relative.ends_with(".parquet") || relative.contains("/consumer/consumer-") {
            let (parent, _) = relative.rsplit_once('/').unwrap();
            if !self.operator.exists(&format!("{parent}/")).await? {
                return Err(paimon::Error::DataInvalid {
                    message: format!("Data file parent must exist: {parent}"),
                    source: None,
                });
            }
        }
        Ok((self.operator.clone(), relative.into()))
    }
}

#[cfg(feature = "storage-fs")]
#[tokio::test]
async fn writes_create_parent_before_strict_provider_resolves_file() {
    let root = tempfile::tempdir().unwrap();
    let mut config = opendal_service_fs::FsConfig::default();
    config.root = Some(root.path().to_str().unwrap().into());
    let operator = Operator::from_config(config).unwrap();
    let provider = Arc::new(StrictParentProvider {
        operator,
        parent_resolutions: Mutex::new(Vec::new()),
    });
    let io = FileIOBuilder::new("unused")
        .with_provider(provider.clone())
        .build()
        .unwrap();

    for (name, primary_key) in [("append", false), ("pk", true)] {
        let table = partitioned_test_table(io.clone(), name, primary_key);
        let bucket = format!("{name}/pt=a%2Fb/bucket-0/");
        assert!(!provider.operator.exists(&bucket).await.unwrap());
        let file = write_partitioned_test_file(&table).await;
        assert!(
            provider
                .operator
                .stat(&format!("{bucket}{file}"))
                .await
                .unwrap()
                .content_length()
                > 0
        );

        let manager = table.consumer_manager();
        manager.reset("test", 1).await.unwrap();
        {
            let consumer = format!("{name}/consumer/");
            let parents = provider.parent_resolutions.lock().unwrap();
            assert_eq!(
                parents
                    .iter()
                    .filter(|path| path.as_str() == bucket.trim_end_matches('/'))
                    .count(),
                1
            );
            assert_eq!(
                parents
                    .iter()
                    .filter(|path| path.as_str() == consumer.trim_end_matches('/'))
                    .count(),
                1
            );
        }
        assert_eq!(manager.get("test").await.unwrap(), Some(1));
    }
}

fn partitioned_test_table(io: paimon::io::FileIO, name: &str, primary_key: bool) -> Table {
    let mut schema = Schema::builder()
        .column(
            "pt",
            DataType::VarChar(paimon::spec::VarCharType::string_type()),
        )
        .column("id", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("bucket", "1")
        .option("bucket-key", "id");
    if primary_key {
        schema = schema.primary_key(["pt", "id"]);
    }
    let path = format!("s3://bucket/{name}");
    Table::new(
        io,
        Identifier::new("default", name),
        path.clone(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    )
}

async fn write_partitioned_test_file(table: &Table) -> String {
    let batch = RecordBatch::try_from_iter([
        (
            "pt",
            Arc::new(arrow_array::StringArray::from(vec!["a/b"])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
    ])
    .unwrap();
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    messages[0].new_files[0].file_name.clone()
}

#[cfg(feature = "storage-s3")]
#[derive(Debug)]
struct SharedS3Provider(Operator);

#[cfg(feature = "storage-s3")]
#[async_trait::async_trait]
impl FileIOProvider for SharedS3Provider {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        Ok((
            self.0.clone(),
            path.strip_prefix("s3://bucket/").unwrap().into(),
        ))
    }
}

#[cfg(feature = "storage-s3")]
#[tokio::test]
async fn s3_writes_put_only_required_objects() {
    use axum::{
        body::Bytes,
        http::{Method, StatusCode, Uri},
        Router,
    };

    let puts = Arc::new(Mutex::new(Vec::new()));
    let captured = puts.clone();
    let app = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
        let captured = captured.clone();
        async move {
            if method == Method::PUT {
                captured
                    .lock()
                    .unwrap()
                    .push((uri.path().to_string(), body));
                (StatusCode::OK, "")
            } else if method == Method::GET
                && uri.query().is_some_and(|q| q.contains("list-type=2"))
            {
                (
                    StatusCode::OK,
                    "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
                )
            } else {
                (StatusCode::NOT_FOUND, "")
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let io = FileIOBuilder::new("s3")
        .with_prop("s3.endpoint", &endpoint)
        .with_prop("s3.region", "us-east-1")
        .with_prop("s3.access-key", "test")
        .with_prop("s3.secret-key", "test")
        .with_prop("s3.path-style-access", "true")
        .build()
        .unwrap();
    for (name, primary_key) in [("append", false), ("pk", true)] {
        let table = partitioned_test_table(io.clone(), name, primary_key);
        let file = write_partitioned_test_file(&table).await;
        let mut requests = puts.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let (key, body) = requests.pop().unwrap();
        assert!(key.ends_with(&file), "{key}");
        assert!(body.starts_with(b"PAR1") && body.ends_with(b"PAR1"));
    }
    let mut config = opendal_service_s3::S3Config::default();
    config.bucket = "bucket".into();
    config.endpoint = Some(endpoint);
    config.region = Some("us-east-1".into());
    config.skip_signature = true;
    let transport =
        opendal::HttpTransporter::new(opendal_http_transport_reqwest::ReqwestTransport::default());
    let operator = Operator::from_config(config)
        .unwrap()
        .with_context(opendal::OperationContext::new().with_http_transport(transport));
    let shared = FileIOBuilder::new("unused")
        .with_provider(Arc::new(SharedS3Provider(operator)))
        .build()
        .unwrap();
    partitioned_test_table(shared, "shared", false)
        .consumer_manager()
        .reset("test", 1)
        .await
        .unwrap();
    let requests = puts.lock().unwrap();
    let keys: Vec<_> = requests.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(
        keys,
        [
            "/bucket/shared/consumer/",
            "/bucket/shared/consumer/consumer-test"
        ]
    );
    server.abort();
}
