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

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::object_store::{memory::InMemory, path::Path, ObjectStoreExt};
use paimon_datafusion::SQLContext;

async fn context(files: &[(&str, &[u8])]) -> (SQLContext, Arc<InMemory>) {
    let store = Arc::new(InMemory::new());
    for (path, bytes) in files {
        store
            .put(&Path::from(*path), bytes.to_vec().into())
            .await
            .unwrap();
    }
    let ctx = SQLContext::new();
    ctx.ctx()
        .runtime_env()
        .register_object_store(&url::Url::parse("memory://files").unwrap(), store.clone());
    ctx.register_read_files_location("memory://files/allowed")
        .unwrap();
    (ctx, store)
}

async fn rows(ctx: &SQLContext, sql: &str) -> Vec<Vec<ScalarValue>> {
    ctx.sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| ScalarValue::try_from_array(column, row).unwrap())
                        .collect()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn fails(ctx: &SQLContext, sql: &str, expected: &str) {
    let message = match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame.collect().await.unwrap_err().to_string(),
    };
    assert!(
        message.contains(expected),
        "expected {expected:?}, got {message}"
    );
}

fn text(value: &ScalarValue) -> &str {
    value.try_as_str().flatten().unwrap()
}

#[tokio::test]
async fn read_files_rejects_unregistered_locations() {
    let ctx = SQLContext::new();
    let error = ctx
        .sql("SELECT * FROM read_files('file:///etc/hostname', format => 'text')")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("read_files: location is not registered"),
        "{error}"
    );
}

#[tokio::test]
async fn csv_named_options_schema_and_constant_arguments() {
    let (ctx, _) = context(&[("allowed/data.csv", b"id;name\n1;Alice\n2;Bob\n")]).await;
    let result = rows(&ctx, "SELECT id, name FROM read_files(concat('memory://files/', 'allowed/data.csv'), schema => 'id BIGINT, name STRING', sep => ';', header => true, format => 'csv') WHERE id > 1").await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], ScalarValue::Int64(Some(2)));
    assert_eq!(text(&result[0][1]), "Bob");
}

#[tokio::test]
async fn csv_infers_headers_and_explicit_schema_defaults_to_headerless() {
    let (ctx, _) = context(&[
        ("allowed/header.csv", b"id,name\n1,Alice\n"),
        ("allowed/rows.csv", b"2,Bob\n3,Carol\n"),
    ])
    .await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM read_files('memory://files/allowed/header.csv')"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(1))]]
    );
    let result = rows(&ctx, "SELECT id FROM read_files('memory://files/allowed/rows.csv', format => 'csv', schema => 'id BIGINT, name STRING') ORDER BY id").await;
    assert_eq!(
        result,
        vec![
            vec![ScalarValue::Int64(Some(2))],
            vec![ScalarValue::Int64(Some(3))]
        ]
    );
}

#[tokio::test]
async fn csv_quoted_multiline_and_null_value() {
    let (ctx, _) = context(&[("allowed/data.csv", b"id,name\n1,\"hello\nworld\"\n2,NA\n")]).await;
    let result = rows(&ctx, "SELECT name FROM read_files('memory://files/allowed/data.csv', format => 'csv', multiLine => true, nullValue => 'NA') ORDER BY id").await;
    assert_eq!(text(&result[0][0]), "hello\nworld");
    assert!(result[1][0].is_null());
}

#[tokio::test]
async fn parquet_keeps_types_and_queries_as_a_relation() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["one", "two"])),
        ],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let (ctx, _) = context(&[("allowed/data.parquet", &bytes)]).await;
    assert_eq!(rows(&ctx, "WITH files AS (SELECT id FROM read_files('memory://files/allowed/data.parquet')) SELECT files.id FROM files JOIN (VALUES (2)) t(id) ON files.id = t.id").await, vec![vec![ScalarValue::Int64(Some(2))]]);
}

#[tokio::test]
async fn json_lines_extensions_and_multiline_documents() {
    let (ctx, _) = context(&[
        ("allowed/events.ndjson", b"{\"id\":1}\n{\"id\":2}\n"),
        ("allowed/object.json", b"{\n \"id\": 3\n}"),
        ("allowed/array.json", b"[\n{\"id\":4},\n{\"id\":5}\n]"),
    ])
    .await;
    assert_eq!(rows(&ctx, "SELECT sum(id) FROM read_files('memory://files/allowed/events.ndjson', format => 'json')").await, vec![vec![ScalarValue::Int64(Some(3))]]);
    assert_eq!(rows(&ctx, "SELECT id FROM read_files('memory://files/allowed/object.json', format => 'json', multiLine => true)").await, vec![vec![ScalarValue::Int64(Some(3))]]);
    assert_eq!(rows(&ctx, "SELECT sum(id) FROM read_files('memory://files/allowed/array.json', multiLine => true)").await, vec![vec![ScalarValue::Int64(Some(9))]]);
    fails(
        &ctx,
        "SELECT * FROM read_files('memory://files/allowed/object.json', format => 'json')",
        "JSON",
    )
    .await;
}

#[tokio::test]
async fn text_lines_whole_file_custom_separator_and_empty_file() {
    let (ctx, _) = context(&[
        ("allowed/lines.txt", "你好\r\n\rworld\nlast".as_bytes()),
        ("allowed/custom.txt", b"a||b||||c||"),
        ("allowed/empty.txt", b""),
    ])
    .await;
    let result = rows(
        &ctx,
        "SELECT value FROM read_files('memory://files/allowed/lines.txt', format => 'text')",
    )
    .await;
    assert_eq!(
        result.iter().map(|row| text(&row[0])).collect::<Vec<_>>(),
        ["你好", "", "world", "last"]
    );
    let result = rows(&ctx, "SELECT value FROM read_files('memory://files/allowed/lines.txt', format => 'text', wholeText => true)").await;
    assert_eq!(text(&result[0][0]), "你好\r\n\rworld\nlast");
    let result = rows(&ctx, "SELECT value FROM read_files('memory://files/allowed/custom.txt', format => 'text', lineSep => '||')").await;
    assert_eq!(
        result.iter().map(|row| text(&row[0])).collect::<Vec<_>>(),
        ["a", "b", "", "c"]
    );
    assert!(rows(
        &ctx,
        "SELECT value FROM read_files('memory://files/allowed/empty.txt', format => 'text')"
    )
    .await
    .is_empty());
    assert_eq!(text(&rows(&ctx, "SELECT value FROM read_files('memory://files/allowed/empty.txt', format => 'text', wholeText => true)").await[0][0]), "");
    assert_eq!(
        rows(
            &ctx,
            "SELECT count(*) FROM read_files('memory://files/allowed/lines.txt', format => 'text')"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(4))]]
    );
}

#[tokio::test]
async fn binary_file_returns_bytes_and_can_skip_content_io() {
    let (ctx, store) = context(&[("allowed/picture", &[0, 255, 7])]).await;
    let modified = store
        .head(&Path::from("allowed/picture"))
        .await
        .unwrap()
        .last_modified
        .timestamp_micros();
    let result = rows(&ctx, "SELECT path, length, content, _metadata.file_name, \"modificationTime\" FROM read_files('memory://files/allowed/picture', format => 'binaryFile')").await;
    assert_eq!(text(&result[0][0]), "memory://files/allowed/picture");
    assert_eq!(result[0][1], ScalarValue::Int64(Some(3)));
    assert_eq!(result[0][2], ScalarValue::Binary(Some(vec![0, 255, 7])));
    assert_eq!(text(&result[0][3]), "picture");
    assert_eq!(
        result[0][4],
        ScalarValue::TimestampMicrosecond(Some(modified), None)
    );

    // Plan while the file exists, then remove it. A metadata-only projection
    // must still execute, while a content projection must preserve the error.
    let metadata = ctx.sql("SELECT length, _metadata.file_name FROM read_files('memory://files/allowed/picture', format => 'binaryFile')").await.unwrap();
    let content = ctx.sql("SELECT content FROM read_files('memory://files/allowed/picture', format => 'binaryFile')").await.unwrap();
    store.delete(&Path::from("allowed/picture")).await.unwrap();
    let result = metadata.collect().await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(content.collect().await.is_err());
}

#[tokio::test]
async fn directory_glob_filter_and_unified_json_schema() {
    let (ctx, _) = context(&[
        ("allowed/events/a.jsonl", b"{\"id\":1}\n"),
        (
            "allowed/events/nested/b.ndjson",
            b"{\"id\":2,\"name\":\"two\"}\n",
        ),
        ("allowed/events/_ignored.jsonl", b"invalid"),
        ("allowed/events/.hidden/c.jsonl", b"invalid"),
    ])
    .await;
    let result = rows(&ctx, "SELECT id, name FROM read_files('memory://files/allowed/events/', format => 'json') ORDER BY id").await;
    assert_eq!(result.len(), 2);
    assert!(result[0][1].is_null());
    assert_eq!(text(&result[1][1]), "two");
    assert_eq!(rows(&ctx, "SELECT id FROM read_files('memory://files/allowed/events/*', format => 'json', fileNamePattern => '*.ndjson')").await, vec![vec![ScalarValue::Int64(Some(2))]]);
    fails(
        &ctx,
        "SELECT * FROM read_files('memory://files/allowed/events/*.csv', format => 'csv')",
        "no files matched",
    )
    .await;
}

#[tokio::test]
async fn rejects_invalid_options_and_unsupported_sources() {
    let (ctx, _) = context(&[("allowed/a.txt", b"a")]).await;
    for (args, expected) in [
        (
            "'memory://files/allowed/a.txt', 'text'",
            "options must be named",
        ),
        (
            "'memory://files/allowed/a.txt', format => 'text', FORMAT => 'text'",
            "duplicate option",
        ),
        (
            "'memory://files/allowed/a.txt', sep => ';', delimiter => ','",
            "duplicate option",
        ),
        (
            "'memory://files/allowed/a.txt', typo => true",
            "unsupported option",
        ),
        (
            "'memory://files/allowed/a.txt', format => 'xml'",
            "unsupported format",
        ),
        (
            "'memory://files/allowed/a.txt', format => 'text', header => true",
            "not valid",
        ),
        (
            "'memory://files/allowed/a.txt', format => 'text', lineSep => ''",
            "must not be empty",
        ),
        ("NULL", "non-null STRING"),
        (
            "'https://example.com/a.csv', format => 'csv'",
            "HTTP and HTTPS",
        ),
        (
            "'http://example.com/a.csv', format => 'csv'",
            "HTTP and HTTPS",
        ),
        (
            "'memory://files/allowed-other/a.txt', format => 'text'",
            "location is not registered",
        ),
        (
            "'memory://files/allowed/../private/a.txt', format => 'text'",
            "location is not registered",
        ),
        (
            "'memory://other/allowed/a.txt', format => 'text'",
            "location is not registered",
        ),
    ] {
        fails(&ctx, &format!("SELECT * FROM read_files({args})"), expected).await;
    }
    for name in [
        "read_csv",
        "read_parquet",
        "read_json",
        "read_text",
        "read_blob",
        "read_ndjson",
    ] {
        fails(
            &ctx,
            &format!("SELECT * FROM {name}('memory://files/allowed/a.txt')"),
            "not found",
        )
        .await;
    }
}

#[tokio::test]
async fn local_files_require_registration_and_symlinks_do_not_escape() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("data.txt"), "inside").unwrap();
    std::fs::write(outside.path().join("secret.txt"), "outside").unwrap();
    let ctx = SQLContext::new();
    ctx.register_read_files_location(directory.path().to_str().unwrap())
        .unwrap();
    let path = directory.path().join("data.txt");
    let result = rows(
        &ctx,
        &format!(
            "SELECT value FROM read_files('{}', format => 'text')",
            path.display()
        ),
    )
    .await;
    assert_eq!(text(&result[0][0]), "inside");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            outside.path().join("secret.txt"),
            directory.path().join("link.txt"),
        )
        .unwrap();
        fails(
            &ctx,
            &format!(
                "SELECT * FROM read_files('{}/link.txt', format => 'text')",
                directory.path().display()
            ),
            "location is not registered",
        )
        .await;
    }
}

#[tokio::test]
async fn file_io_adapter_reads_ranges_lists_and_preserves_literal_keys() {
    let io = paimon::io::FileIOBuilder::new("memory").build().unwrap();
    for (path, bytes) in [
        (
            "memory://fileio/allowed/a.txt",
            b"first\nsecond\n".as_slice(),
        ),
        (
            "memory://fileio/allowed/nested/space %.txt",
            b"literal".as_slice(),
        ),
    ] {
        io.new_output(path)
            .unwrap()
            .write(bytes.to_vec().into())
            .await
            .unwrap();
    }
    let ctx = SQLContext::new();
    ctx.register_read_files_location_with_file_io("memory://fileio/allowed", io)
        .unwrap();
    let result = rows(
        &ctx,
        "SELECT value FROM read_files('memory://fileio/allowed/a.txt', format => 'text')",
    )
    .await;
    assert_eq!(
        result.iter().map(|row| text(&row[0])).collect::<Vec<_>>(),
        ["first", "second"]
    );
    let result = rows(&ctx, "SELECT value, _metadata.file_name FROM read_files('memory://fileio/allowed/nested/', format => 'text')").await;
    assert_eq!(text(&result[0][0]), "literal");
    assert_eq!(text(&result[0][1]), "space %.txt");
    let listing =
        datafusion::datasource::listing::ListingTableUrl::parse("memory://fileio/allowed/")
            .unwrap();
    // Registration belongs to this SQLContext, not its shared RuntimeEnv.
    assert!(ctx.ctx().runtime_env().object_store(&listing).is_err());
}

#[tokio::test]
async fn file_io_adapter_scans_parquet_with_range_reads() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let io = paimon::io::FileIOBuilder::new("memory").build().unwrap();
    io.new_output("memory://parquet/allowed/file.parquet")
        .unwrap()
        .write(writer.into_inner().unwrap().into())
        .await
        .unwrap();
    let ctx = SQLContext::new();
    ctx.register_read_files_location_with_file_io("memory://parquet/allowed/", io)
        .unwrap();
    assert_eq!(
        rows(
            &ctx,
            "SELECT sum(id) FROM read_files('memory://parquet/allowed/file.parquet')"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(30))]]
    );
}

#[tokio::test]
async fn text_stream_preserves_chunk_boundaries_and_batch_counts() {
    let ctx = SQLContext::new();
    ctx.ctx()
        .sql("SET datafusion.execution.batch_size = 2")
        .await
        .unwrap();
    let io = paimon::io::FileIOBuilder::new("memory").build().unwrap();
    // The FileIO adapter returns 1 MiB chunks. Place CRLF across that boundary.
    let first = "a".repeat(1024 * 1024 - 1);
    let input = format!("{first}\r\n你好\nlast");
    io.new_output("memory://chunks/allowed/text.txt")
        .unwrap()
        .write(input.into_bytes().into())
        .await
        .unwrap();
    ctx.register_read_files_location_with_file_io("memory://chunks/allowed", io)
        .unwrap();
    let batches = ctx
        .sql("SELECT value FROM read_files('memory://chunks/allowed/text.txt', format => 'text')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let values = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|r| ScalarValue::try_from_array(batch.column(0), r).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(text(&values[0]), first);
    assert_eq!(text(&values[1]), "你好");
    assert_eq!(text(&values[2]), "last");
}

#[tokio::test]
async fn read_files_works_inside_temporary_views_and_explain() {
    let (mut ctx, _) = context(&[("allowed/events.json", b"{\"id\":1}\n{\"id\":2}\n")]).await;
    let warehouse = tempfile::tempdir().unwrap();
    let mut options = paimon::Options::new();
    options.set(
        paimon::CatalogOptions::WAREHOUSE,
        warehouse.path().to_str().unwrap(),
    );
    ctx.register_catalog(
        "paimon",
        Arc::new(paimon::FileSystemCatalog::new(options).unwrap()),
    )
    .await
    .unwrap();
    ctx.sql("CREATE TEMPORARY VIEW events AS SELECT id FROM read_files('memory://files/allowed/events.json')").await.unwrap();
    assert_eq!(
        rows(&ctx, "SELECT sum(id) FROM events").await,
        vec![vec![ScalarValue::Int64(Some(3))]]
    );
    assert!(!rows(
        &ctx,
        "EXPLAIN SELECT id FROM read_files('memory://files/allowed/events.json') WHERE id = 2"
    )
    .await
    .is_empty());
    for schema in [
        "id BIGINT, id STRING",
        "id BIGINT DEFAULT 1",
        "id BIGINT); DROP TABLE events; --",
    ] {
        fails(&ctx, &format!("SELECT * FROM read_files('memory://files/allowed/events.json', schema => '{schema}')"), "schema").await;
    }
}

#[tokio::test]
async fn file_io_permission_errors_are_not_treated_as_missing_files() {
    #[derive(Debug)]
    struct Denied;
    #[async_trait::async_trait]
    impl paimon::io::FileIOProvider for Denied {
        async fn create(&self, _: &str) -> paimon::Result<(opendal::Operator, String)> {
            Err(paimon::Error::ConfigInvalid {
                message: "read access denied by provider".into(),
            })
        }
    }
    let io = paimon::io::FileIOBuilder::new("memory")
        .with_provider(Arc::new(Denied))
        .build()
        .unwrap();
    let ctx = SQLContext::new();
    ctx.register_read_files_location_with_file_io("memory://denied/allowed", io)
        .unwrap();
    fails(
        &ctx,
        "SELECT * FROM read_files('memory://denied/allowed/a.csv', format => 'csv')",
        "read access denied by provider",
    )
    .await;
}
