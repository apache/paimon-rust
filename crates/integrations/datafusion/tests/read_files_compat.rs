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

use datafusion::common::ScalarValue;
use datafusion::object_store::{memory::InMemory, path::Path, ObjectStoreExt};
use paimon_datafusion::SQLContext;

async fn context(files: &[(&str, &[u8])]) -> SQLContext {
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
        .register_object_store(&url::Url::parse("memory://files").unwrap(), store);
    ctx.register_read_files_location("memory://files/allowed")
        .unwrap();
    ctx
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
    assert!(message.contains(expected), "{message}");
}

fn text(value: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(value.to_string()))
}

#[tokio::test]
async fn csv_defaults_to_backslash_escape_and_allows_override() {
    let ctx = context(&[
        ("allowed/default.csv", b"id,name\n1,\"a\\\"b\"\n"),
        ("allowed/custom.csv", b"id,name\n1,\"a!\"b\"\n"),
        (
            "allowed/paths.csv",
            b"id,\"na\\\"me\",\"C:\\header\"\r\n1,\"a\\\"b\",\"C:\\temp\\file\"\r\n",
        ),
    ])
    .await;
    for options in ["", ", nullValue => 'NA'"] {
        assert_eq!(
            rows(
                &ctx,
                &format!(
                    "SELECT name FROM read_files('memory://files/allowed/default.csv'{options})"
                )
            )
            .await,
            vec![vec![text("a\"b")]]
        );
    }
    assert_eq!(
        rows(
            &ctx,
            "SELECT name FROM read_files('memory://files/allowed/custom.csv', escape => '!')"
        )
        .await,
        vec![vec![text("a\"b")]]
    );
    assert_eq!(
        rows(
            &ctx,
            r#"SELECT "na""me", "C:\header" FROM read_files('memory://files/allowed/paths.csv')"#
        )
        .await,
        vec![vec![text("a\"b"), text("C:\\temp\\file")]]
    );
}

#[tokio::test]
async fn csv_multiline_is_opt_in_and_respects_quoting() {
    let ctx = context(&[
        ("allowed/newline.csv", b"id,name\n1,\"hello\nworld\"\n"),
        ("allowed/carriage.csv", b"id,name\r\n1,\"hello\rworld\"\r\n"),
        (
            "allowed/quotes.csv",
            b"id,name\n1,\"a\\\"b\"\n2,\"a\"\"b\"\n3,a\"b\n",
        ),
    ])
    .await;
    for file in ["newline", "carriage"] {
        for options in [
            "",
            ", multiLine => false",
            ", nullValue => 'NA'",
            ", schema => 'id BIGINT, name STRING', header => true",
        ] {
            fails(
                &ctx,
                &format!("SELECT * FROM read_files('memory://files/allowed/{file}.csv'{options})"),
                "set multiLine => true",
            )
            .await;
        }
    }
    assert_eq!(
        rows(
            &ctx,
            "SELECT name FROM read_files('memory://files/allowed/newline.csv', multiLine => true)"
        )
        .await,
        vec![vec![text("hello\nworld")]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT name FROM read_files('memory://files/allowed/quotes.csv') ORDER BY id"
        )
        .await,
        vec![vec![text("a\"b")], vec![text("a\"b")], vec![text("a\"b")]]
    );
}

#[tokio::test]
async fn csv_streams_multiple_batches_with_projection_and_limit() {
    let mut bytes = b"id,name\n".to_vec();
    for id in 0..10_000 {
        bytes.extend_from_slice(format!("{id},row{id}\n").as_bytes());
    }
    let ctx = context(&[("allowed/data.csv", &bytes)]).await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT count(*), sum(id) FROM read_files('memory://files/allowed/data.csv')"
        )
        .await,
        vec![vec![
            ScalarValue::Int64(Some(10_000)),
            ScalarValue::Int64(Some(49_995_000))
        ]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT count(*) FROM read_files('memory://files/allowed/data.csv')"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(10_000))]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT name FROM read_files('memory://files/allowed/data.csv') LIMIT 2"
        )
        .await,
        vec![vec![text("row0")], vec![text("row1")]]
    );
}

#[tokio::test]
async fn csv_widens_numeric_types_across_files() {
    let ctx = context(&[("allowed/a.csv", b"v\n1\n"), ("allowed/b.csv", b"v\n1.5\n")]).await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM read_files('memory://files/allowed/') ORDER BY v"
        )
        .await,
        vec![
            vec![ScalarValue::Float64(Some(1.0))],
            vec![ScalarValue::Float64(Some(1.5))]
        ]
    );
    let ctx = context(&[
        ("allowed/a.csv", b"z,a\n1,2\n"),
        ("allowed/b.csv", b"a,z\n3.5,4\n"),
    ])
    .await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT z, a FROM read_files('memory://files/allowed/') ORDER BY z"
        )
        .await,
        vec![
            vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Int64(Some(2))],
            vec![ScalarValue::Float64(Some(3.5)), ScalarValue::Int64(Some(4))],
        ]
    );
    let ctx = context(&[
        ("allowed/a.csv", b"v\n2025-01-01\n"),
        ("allowed/b.csv", b"v\nunknown\n"),
    ])
    .await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM read_files('memory://files/allowed/') ORDER BY v"
        )
        .await,
        vec![vec![text("2025-01-01")], vec![text("unknown")]]
    );
}

#[tokio::test]
async fn ndjson_streams_coerced_primitives_with_bounded_inference() {
    let mut large = Vec::new();
    for id in 0..10_000 {
        large.extend_from_slice(format!("{{\"id\":{id},\"extra\":\"ignored\"}}\n").as_bytes());
    }
    let mut bounded = b"{\"v\":1}\n".repeat(1_000);
    bounded.extend_from_slice(b"invalid JSON beyond the inference sample\n");
    let ctx = context(&[
        (
            "allowed/primitives/a.ndjson",
            b"{\"id\":1,\"v\":1}\n{\"id\":2,\"v\":\"word\"}\n",
        ),
        ("allowed/primitives/b.ndjson", b"{\"id\":3,\"v\":false}\n"),
        ("allowed/numbers/a.ndjson", b"{\"v\":1}\n"),
        ("allowed/numbers/b.ndjson", b"{\"v\":1.5}\n"),
        ("allowed/large.ndjson", &large),
        ("allowed/bounded.ndjson", &bounded),
    ])
    .await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM read_files('memory://files/allowed/primitives/') ORDER BY id"
        )
        .await,
        vec![vec![text("1")], vec![text("word")], vec![text("false")]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM read_files('memory://files/allowed/numbers/') ORDER BY v"
        )
        .await,
        vec![
            vec![ScalarValue::Float64(Some(1.0))],
            vec![ScalarValue::Float64(Some(1.5))]
        ]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT count(*), sum(id) FROM read_files('memory://files/allowed/large.ndjson')"
        )
        .await,
        vec![vec![
            ScalarValue::Int64(Some(10_000)),
            ScalarValue::Int64(Some(49_995_000))
        ]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT count(*) FROM read_files('memory://files/allowed/large.ndjson')"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(10_000))]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM read_files('memory://files/allowed/large.ndjson') LIMIT 2"
        )
        .await,
        vec![
            vec![ScalarValue::Int64(Some(0))],
            vec![ScalarValue::Int64(Some(1))]
        ]
    );
    // Planning parses only the first 1,000 records, and a LIMIT can stop
    // execution before the malformed record outside the inference sample.
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM read_files('memory://files/allowed/bounded.ndjson') LIMIT 1"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(1))]]
    );
}

#[tokio::test]
async fn ndjson_enforces_physical_records_with_explicit_schema_and_sampled_tails() {
    let mut tail = b"{\"id\":1}\n".repeat(1_000);
    tail.extend_from_slice(b"{\n\"id\":2\n}\n");
    let ctx = context(&[
        ("allowed/multiline.json", b"{\n\"id\":1\n}\n"),
        ("allowed/concatenated.json", b"{\"id\":1}{\"id\":2}\n"),
        ("allowed/tail.json", &tail),
        ("allowed/carriage.json", b"{\"id\":1}\r{\"id\":2}\r"),
        (
            "allowed/escaped.json",
            b"{\"id\":1,\"v\":\"quote\\\"} bracket] and newline\\n\"}\r\n",
        ),
    ])
    .await;
    fails(
        &ctx,
        "SELECT * FROM read_files('memory://files/allowed/multiline.json', schema => 'id BIGINT')",
        "set multiLine => true",
    )
    .await;
    fails(&ctx, "SELECT * FROM read_files('memory://files/allowed/concatenated.json', schema => 'id BIGINT')", "one object per line").await;
    fails(
        &ctx,
        "SELECT * FROM read_files('memory://files/allowed/tail.json')",
        "set multiLine => true",
    )
    .await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM read_files('memory://files/allowed/escaped.json')"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(1))]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM read_files('memory://files/allowed/tail.json') LIMIT 1"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(1))]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT sum(id) FROM read_files('memory://files/allowed/carriage.json')"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(3))]]
    );
}

#[tokio::test]
async fn multiline_json_infers_one_widened_schema_across_files() {
    let ctx = context(&[
        (
            "allowed/numbers/a.json",
            br#"{"v":1,"nested":{"x":2,"left":true},"items":[1]}"#,
        ),
        (
            "allowed/numbers/b.json",
            br#"{"v":1.5,"nested":{"x":2.5,"right":"ok"},"items":[1.5]}"#,
        ),
        ("allowed/strings/a.json", br#"{"v":1}"#),
        ("allowed/strings/b.json", br#"{"v":"word"}"#),
        ("allowed/nulls/a.json", br#"{"v":null}"#),
        ("allowed/nulls/b.json", br#"{"v":1}"#),
    ])
    .await;
    assert_eq!(
        rows(&ctx, "SELECT v, nested.x, items[1] FROM read_files('memory://files/allowed/numbers/', multiLine => true) ORDER BY v").await,
        vec![
            vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(2.0)), ScalarValue::Float64(Some(1.0))],
            vec![ScalarValue::Float64(Some(1.5)), ScalarValue::Float64(Some(2.5)), ScalarValue::Float64(Some(1.5))],
        ]
    );
    assert_eq!(
        rows(&ctx, "SELECT nested.left, nested.right FROM read_files('memory://files/allowed/numbers/', multiLine => true) ORDER BY v").await,
        vec![
            vec![ScalarValue::Boolean(Some(true)), ScalarValue::Utf8(None)],
            vec![ScalarValue::Boolean(None), text("ok")],
        ]
    );
    assert_eq!(
        rows(&ctx, "SELECT v FROM read_files('memory://files/allowed/strings/', multiLine => true) ORDER BY v").await,
        vec![vec![text("1")], vec![text("word")]]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT sum(v) FROM read_files('memory://files/allowed/nulls/', multiLine => true)"
        )
        .await,
        vec![vec![ScalarValue::Int64(Some(1))]]
    );
}

#[tokio::test]
async fn source_metadata_column_takes_precedence() {
    let ctx = context(&[
        ("allowed/data.csv", b"id,_metadata\n1,business\n"),
        ("allowed/data.json", br#"{"id":1,"_metadata":"business"}"#),
    ])
    .await;
    for options in ["", ", nullValue => 'NA'"] {
        assert_eq!(
            rows(
                &ctx,
                &format!(
                    "SELECT _metadata FROM read_files('memory://files/allowed/data.csv'{options})"
                )
            )
            .await,
            vec![vec![text("business")]]
        );
    }
    assert_eq!(
        rows(&ctx, "SELECT _metadata FROM read_files('memory://files/allowed/data.json', multiLine => true)").await,
        vec![vec![text("business")]]
    );
}

#[tokio::test]
async fn strict_globs_match_directories_and_preserve_path_depth() {
    let ctx = context(&[
        ("allowed/root.csv", b"id\n1\n"),
        ("allowed/b/c/a.csv", b"id\n2\n"),
        ("allowed/b/c/d/b.csv", b"id\n3\n"),
        ("allowed/b/x/y/c/c.csv", b"id\n4\n"),
        ("allowed/b/cookie/d.csv", b"id\n5\n"),
        ("allowed/prefix.csv", b"id\n6\n"),
        ("allowed/prefix/child.csv", b"id\n7\n"),
    ])
    .await;
    assert_eq!(
        rows(
            &ctx,
            "SELECT id FROM read_files('memory://files/allowed/*.csv') ORDER BY id"
        )
        .await,
        vec![
            vec![ScalarValue::Int64(Some(1))],
            vec![ScalarValue::Int64(Some(6))]
        ]
    );
    for pattern in ["*/c/", "*/c"] {
        assert_eq!(
            rows(&ctx, &format!("SELECT id FROM read_files('memory://files/allowed/{pattern}', format => 'csv') ORDER BY id")).await,
            vec![vec![ScalarValue::Int64(Some(2))], vec![ScalarValue::Int64(Some(3))]]
        );
    }
    assert_eq!(
        rows(&ctx, "SELECT id FROM read_files('memory://files/allowed/prefix*', format => 'csv') ORDER BY id").await,
        vec![vec![ScalarValue::Int64(Some(6))], vec![ScalarValue::Int64(Some(7))]]
    );
}
