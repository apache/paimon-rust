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

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::object_store::{memory::InMemory, path::Path, ObjectStoreExt};
use paimon::io::{FileIO, FileIOBuilder};
use paimon_datafusion::SQLContext;
use std::sync::Arc;

async fn io(value: &str) -> FileIO {
    let io = FileIOBuilder::new("memory").build().unwrap();
    io.new_output("memory://tenant/allowed/data.csv")
        .unwrap()
        .write(format!("value\n{value}\n").into_bytes().into())
        .await
        .unwrap();
    io
}

async fn value(ctx: &SQLContext) -> String {
    let batches = ctx
        .sql("SELECT value FROM read_files('memory://tenant/allowed/data.csv', format => 'csv')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ScalarValue::try_from_array(batches[0].column(0), 0)
        .unwrap()
        .try_as_str()
        .flatten()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn file_io_credentials_remain_isolated_with_a_shared_runtime() {
    let runtime = Arc::new(RuntimeEnv::default());
    let a = SQLContext::builder()
        .with_runtime_env(runtime.clone())
        .build();
    let b = SQLContext::builder().with_runtime_env(runtime).build();
    a.register_read_files_location_with_file_io("memory://tenant/allowed", io("TENANT_A").await)
        .unwrap();
    let planned_a = a
        .sql("SELECT value FROM read_files('memory://tenant/allowed/data.csv')")
        .await
        .unwrap();
    b.register_read_files_location_with_file_io("memory://tenant/allowed", io("TENANT_B").await)
        .unwrap();
    assert_eq!(value(&a).await, "TENANT_A");
    assert_eq!(value(&b).await, "TENANT_B");
    let batches = planned_a.collect().await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0)
            .unwrap()
            .try_as_str()
            .flatten(),
        Some("TENANT_A")
    );
    let joined = a.sql("SELECT a.value, b.value FROM read_files('memory://tenant/allowed/data.csv') a CROSS JOIN read_files('memory://tenant/allowed/data.csv') b").await.unwrap().collect().await.unwrap();
    assert_eq!(joined.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn file_io_registration_preserves_the_callers_object_store() {
    let ctx = SQLContext::new();
    let original = Arc::new(InMemory::new());
    original
        .put(&Path::from("other/data.txt"), "original".into())
        .await
        .unwrap();
    let url = url::Url::parse("memory://tenant").unwrap();
    ctx.ctx()
        .runtime_env()
        .register_object_store(&url, original);
    ctx.register_read_files_location_with_file_io("memory://tenant/allowed", io("private").await)
        .unwrap();
    let store = ctx
        .ctx()
        .runtime_env()
        .object_store_registry
        .get_store(&url)
        .unwrap();
    assert_eq!(
        store
            .get(&Path::from("other/data.txt"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"original"
    );
    store
        .put(&Path::from("other/new.txt"), "still writable".into())
        .await
        .unwrap();
}

#[tokio::test]
async fn parquet_scans_keep_their_credentials_and_metadata_separate() {
    let runtime = Arc::new(RuntimeEnv::default());
    let a = SQLContext::builder()
        .with_runtime_env(runtime.clone())
        .build();
    let b = SQLContext::builder().with_runtime_env(runtime).build();
    let mut sizes = Vec::new();
    for (ctx, column, values) in [(&a, "tenant_a", vec![1, 2]), (&b, "tenant_b", vec![3, 4])] {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                column,
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        sizes.push(bytes.len());
        io.new_output("memory://tenant/allowed/data.parquet")
            .unwrap()
            .write(bytes.into())
            .await
            .unwrap();
        ctx.register_read_files_location_with_file_io("memory://tenant/allowed", io)
            .unwrap();
    }
    // The underlying FileIO metadata also gives both files the epoch mtime.
    // Equal sizes force a collision in an incorrectly shared metadata cache.
    assert_eq!(sizes[0], sizes[1]);
    for _ in 0..2 {
        for (ctx, column, expected) in [(&a, "tenant_a", 3), (&b, "tenant_b", 7)] {
            let batches = ctx
                .sql(&format!(
                    "SELECT sum({column}) FROM read_files('memory://tenant/allowed/data.parquet')"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
                ScalarValue::Int64(Some(expected))
            );
        }
    }
}

#[tokio::test]
async fn registered_stores_are_captured_and_adls_container_names_are_authorized() {
    let ctx = SQLContext::new();
    let url = url::Url::parse("abfss://allowed@account.dfs.core.windows.net/").unwrap();
    let original = Arc::new(InMemory::new());
    original
        .put(&Path::from("imports/data.txt"), "original".into())
        .await
        .unwrap();
    ctx.ctx()
        .runtime_env()
        .register_object_store(&url, original);
    ctx.register_read_files_location("abfss://allowed@account.dfs.core.windows.net/imports/")
        .unwrap();
    let replacement = Arc::new(InMemory::new());
    replacement
        .put(&Path::from("imports/data.txt"), "replacement".into())
        .await
        .unwrap();
    ctx.ctx()
        .runtime_env()
        .register_object_store(&url, replacement);
    let batches = ctx.sql("SELECT value FROM read_files('abfss://allowed@account.dfs.core.windows.net/imports/data.txt', format => 'text')").await.unwrap().collect().await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0)
            .unwrap()
            .try_as_str()
            .flatten(),
        Some("original")
    );
    assert!(ctx.sql("SELECT * FROM read_files('abfss://other@account.dfs.core.windows.net/imports/data.txt', format => 'text')").await.unwrap_err().to_string().contains("not registered"));
}

#[cfg(unix)]
#[tokio::test]
async fn local_discovery_skips_symlink_subtrees_and_accepts_directory_aliases() {
    let allowed = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(allowed.path().join("safe.txt"), "safe").unwrap();
    std::fs::write(outside.path().join("secret.txt"), "secret").unwrap();
    std::os::unix::fs::symlink(outside.path(), allowed.path().join("outside")).unwrap();
    let alias = outside.path().join("alias");
    std::os::unix::fs::symlink(allowed.path(), &alias).unwrap();
    let ctx = SQLContext::new();
    ctx.register_read_files_location(alias.to_str().unwrap())
        .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT value FROM read_files('{}/', format => 'text')",
            alias.display()
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0)
            .unwrap()
            .try_as_str()
            .flatten(),
        Some("safe")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn local_symlink_swaps_cannot_escape_after_planning() {
    for format in ["text", "binaryFile", "csv", "json", "parquet"] {
        let allowed = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let source = allowed.path().join("data");
        let secret = outside.path().join("secret");
        if format == "parquet" {
            for (path, value) in [(&source, 1), (&secret, 999)] {
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::Int64,
                        false,
                    )])),
                    vec![Arc::new(Int64Array::from(vec![value]))],
                )
                .unwrap();
                let mut writer = parquet::arrow::ArrowWriter::try_new(
                    std::fs::File::create(path).unwrap(),
                    batch.schema(),
                    None,
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
            }
        } else {
            let (safe, unsafe_bytes) = match format {
                "csv" => ("value\n1\n", "value\n999\n"),
                "json" => ("{\"value\":1}\n", "{\"value\":999}\n"),
                _ => ("safe", "OUTSIDE_SECRET"),
            };
            std::fs::write(&source, safe).unwrap();
            std::fs::write(&secret, unsafe_bytes).unwrap();
        }
        let ctx = SQLContext::new();
        ctx.register_read_files_location(allowed.path().to_str().unwrap())
            .unwrap();
        let frame = ctx
            .sql(&format!(
                "SELECT * FROM read_files('{}', format => '{format}')",
                source.display()
            ))
            .await
            .unwrap();
        std::fs::remove_file(&source).unwrap();
        std::os::unix::fs::symlink(secret, &source).unwrap();
        assert!(
            frame.collect().await.is_err(),
            "{format} followed a symlink outside the registered directory"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn local_special_files_are_rejected_without_waiting_for_a_writer() {
    let directory = tempfile::tempdir().unwrap();
    let fifo = directory.path().join("pipe");
    assert!(std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let ctx = SQLContext::new();
    ctx.register_read_files_location(directory.path().to_str().unwrap())
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.sql(&format!(
            "SELECT * FROM read_files('{}', format => 'text')",
            fifo.display()
        )),
    )
    .await;
    let error = result
        .expect("opening a FIFO blocked the query")
        .unwrap_err();
    assert!(
        error.to_string().contains("Not a directory")
            || error.to_string().contains("not a regular file"),
        "{error}"
    );
}
