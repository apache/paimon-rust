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
            .put(&Path::parse(*path).unwrap(), bytes.to_vec().into())
            .await
            .unwrap();
    }
    let ctx = SQLContext::new();
    ctx.ctx()
        .runtime_env()
        .register_object_store(&url::Url::parse("memory://files").unwrap(), store);
    ctx
}

async fn columns(ctx: &SQLContext, sql: &str) -> Vec<String> {
    let frame = ctx.sql(sql).await.unwrap();
    let names = frame
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    frame.collect().await.unwrap();
    names
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

#[tokio::test]
async fn generated_metadata_is_hidden_from_wildcards_but_can_be_selected() {
    let ctx = context(&[
        ("data.csv", b"id,name\n1,Alice\n"),
        ("data.json", b"{\"id\":1,\"name\":\"Alice\"}\n"),
        ("data.txt", b"hello\n"),
    ])
    .await;
    for (file, options, expected) in [
        ("data.csv", "", vec!["id", "name"]),
        ("data.json", "", vec!["id", "name"]),
        ("data.txt", "", vec!["value"]),
        (
            "data.txt",
            ", format => 'binaryFile'",
            vec!["path", "modificationTime", "length", "content"],
        ),
    ] {
        for star in ["*", "f.*"] {
            let sql = format!("SELECT {star} FROM read_files('memory://files/{file}'{options}) f");
            assert_eq!(columns(&ctx, &sql).await, expected, "{sql}");
        }
        let sql =
            format!("SELECT *, _metadata FROM read_files('memory://files/{file}'{options}) f");
        let mut expected = expected;
        expected.push("_metadata");
        assert_eq!(columns(&ctx, &sql).await, expected);
        let values = rows(&ctx, &format!("SELECT _metadata.file_name, _metadata.file_block_start, _metadata.file_block_length FROM read_files('memory://files/{file}'{options})")).await;
        assert_eq!(values[0][0], ScalarValue::Utf8(Some(file.into())));
        assert_eq!(values[0][1], ScalarValue::Int64(Some(0)));
        assert!(matches!(values[0][2], ScalarValue::Int64(Some(size)) if size > 0));
    }
}

#[tokio::test]
async fn source_metadata_and_explicitly_exported_metadata_stay_visible() {
    let mut ctx = context(&[
        ("data.json", b"{\"id\":1,\"_metadata\":\"user data\"}\n"),
        ("data.txt", b"hello\n"),
    ])
    .await;
    assert_eq!(
        columns(&ctx, "SELECT * FROM read_files('memory://files/data.json')").await,
        ["_metadata", "id"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT _metadata FROM read_files('memory://files/data.json')"
        )
        .await,
        [vec![ScalarValue::Utf8(Some("user data".into()))]]
    );
    for sql in [
        "SELECT * FROM (SELECT *, _metadata FROM read_files('memory://files/data.txt')) v",
        "WITH v AS (SELECT *, _metadata FROM read_files('memory://files/data.txt')) SELECT * FROM v",
    ] {
        assert_eq!(columns(&ctx, sql).await, ["value", "_metadata"]);
    }
    for sql in [
        "WITH v(value) AS (SELECT * FROM read_files('memory://files/data.txt')) SELECT * FROM v",
        "WITH a(value) AS (SELECT * FROM read_files('memory://files/data.txt')), b AS (SELECT f.* FROM read_files('memory://files/data.txt') f CROSS JOIN a) SELECT * FROM b",
    ] {
        assert_eq!(columns(&ctx, sql).await, ["value"]);
    }
    assert_eq!(columns(&ctx, "SELECT * FROM read_files('memory://files/data.txt') f CROSS JOIN (SELECT 'user' AS _metadata) t").await, ["value", "_metadata"]);
    assert_eq!(columns(&ctx, "SELECT f.*, t.* FROM read_files('memory://files/data.txt') f CROSS JOIN (SELECT 'user' AS _metadata) t").await, ["value", "_metadata"]);
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
    ctx.sql("CREATE TEMPORARY VIEW files AS SELECT * FROM read_files('memory://files/data.txt')")
        .await
        .unwrap();
    assert_eq!(columns(&ctx, "SELECT * FROM files").await, ["value"]);
    ctx.sql(
        "CREATE TEMPORARY TABLE imported AS SELECT * FROM read_files('memory://files/data.txt')",
    )
    .await
    .unwrap();
    rows(
        &ctx,
        "INSERT INTO imported SELECT * FROM read_files('memory://files/data.txt')",
    )
    .await;
    assert_eq!(
        rows(&ctx, "SELECT count(*) FROM imported").await,
        [vec![ScalarValue::Int64(Some(2))]]
    );
}

#[tokio::test]
async fn headerless_csv_inference_uses_spark_column_names() {
    let ctx = context(&[("data.csv", b"1,Alice\n2,Bob\n")]).await;
    assert_eq!(
        columns(
            &ctx,
            "SELECT * FROM read_files('memory://files/data.csv', header => false)"
        )
        .await,
        ["_c0", "_c1"]
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT _c1 FROM read_files('memory://files/data.csv', header => false) ORDER BY _c0"
        )
        .await,
        [
            vec![ScalarValue::Utf8(Some("Alice".into()))],
            vec![ScalarValue::Utf8(Some("Bob".into()))]
        ]
    );
}

#[tokio::test]
async fn glob_negation_uses_hadoop_character_classes() {
    let ctx = context(&[("a.txt", b"a"), ("b.txt", b"b"), ("^.txt", b"caret")]).await;
    for path in ["memory://files/[^a].txt", "memory://files/"] {
        let options = if path.ends_with('/') {
            ", format => 'text', pathGlobFilter => '[^a].txt'"
        } else {
            ""
        };
        assert_eq!(
            rows(
                &ctx,
                &format!("SELECT value FROM read_files('{path}'{options}) ORDER BY value")
            )
            .await,
            [
                vec![ScalarValue::Utf8(Some("b".into()))],
                vec![ScalarValue::Utf8(Some("caret".into()))]
            ]
        );
    }
}

#[tokio::test]
async fn aliases_and_wildcard_options_preserve_visible_columns() {
    let ctx = context(&[
        ("data.csv", b"id,name\n1,Alice\n"),
        ("data.txt", b"hello\n"),
    ])
    .await;
    for sql in [
        "SELECT * FROM \"read_files\"('memory://files/data.txt')",
        "SELECT * FROM `read_files`('memory://files/data.txt')",
        "SELECT * FROM read_files('memory://files/data.txt') f(value)",
        "SELECT f.* FROM read_files('memory://files/data.txt') f(value)",
        "SELECT \"file rows\".* FROM read_files('memory://files/data.txt') AS \"file rows\"",
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<2) SELECT f.* FROM read_files('memory://files/data.txt') f CROSS JOIN t",
    ] {
        assert_eq!(columns(&ctx, sql).await, ["value"], "{sql}");
    }
    assert_eq!(
        columns(
            &ctx,
            "SELECT * EXCEPT (id) FROM read_files('memory://files/data.csv')"
        )
        .await,
        ["name"]
    );
    assert_eq!(columns(&ctx, "SELECT f.* EXCEPT (name), _metadata.file_name AS file_name FROM read_files('memory://files/data.csv') f").await, ["id", "file_name"]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT v FROM read_files('memory://files/data.txt') f(v)"
        )
        .await,
        [vec![ScalarValue::Utf8(Some("hello".into()))]]
    );
}

#[tokio::test]
async fn wildcard_replacements_preserve_aliases_for_outer_queries_and_ctas() {
    let mut ctx = context(&[
        ("data.txt", b"hello\n"),
        ("mixed.json", b"{\"Mixed Name\":\"hello\"}\n"),
    ])
    .await;
    for star in ["*", "f.*"] {
        let query = format!("SELECT {star} REPLACE (upper(value) AS value) FROM read_files('memory://files/data.txt') f");
        assert_eq!(columns(&ctx, &query).await, ["value"]);
        let values = rows(&ctx, &format!("SELECT q.value FROM ({query}) q")).await;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0].try_as_str().flatten(), Some("HELLO"));
    }
    let query = r#"SELECT * REPLACE (upper("Mixed Name") AS "Mixed Name") FROM read_files('memory://files/mixed.json')"#;
    assert_eq!(columns(&ctx, query).await, ["Mixed Name"]);
    let values = rows(&ctx, &format!(r#"SELECT q."Mixed Name" FROM ({query}) q"#)).await;
    assert_eq!(values[0][0].try_as_str().flatten(), Some("HELLO"));

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
    ctx.sql("CREATE TEMPORARY TABLE replaced AS SELECT * REPLACE (upper(value) AS value) FROM read_files('memory://files/data.txt')").await.unwrap();
    assert_eq!(columns(&ctx, "SELECT * FROM replaced").await, ["value"]);
    let values = rows(&ctx, "SELECT value FROM replaced").await;
    assert_eq!(values[0][0].try_as_str().flatten(), Some("HELLO"));
}

#[tokio::test]
async fn distinct_star_uses_data_columns_and_parquet_hides_metadata() {
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let ctx = context(&[
        ("duplicates/a.txt", b"same\n"),
        ("duplicates/b.txt", b"same\n"),
        ("data.parquet", &bytes),
    ])
    .await;
    assert_eq!(
        columns(
            &ctx,
            "SELECT * FROM read_files('memory://files/data.parquet')"
        )
        .await,
        ["id"]
    );
    assert_eq!(
        columns(
            &ctx,
            "SELECT *, _metadata FROM read_files('memory://files/data.parquet')"
        )
        .await,
        ["id", "_metadata"]
    );
    assert_eq!(rows(&ctx, "SELECT count(*) FROM (SELECT DISTINCT * FROM read_files('memory://files/duplicates/')) v").await, [vec![ScalarValue::Int64(Some(1))]]);
    assert_eq!(rows(&ctx, "SELECT count(DISTINCT _metadata.file_name) FROM read_files('memory://files/duplicates/')").await, [vec![ScalarValue::Int64(Some(2))]]);
    let error = ctx.sql("SELECT * FROM (read_files('memory://files/duplicates/a.txt') f CROSS JOIN (SELECT 1 AS n) t) j").await.unwrap_err().to_string();
    assert!(
        error.contains("alias the individual sources instead"),
        "{error}"
    );
}

#[tokio::test]
async fn parquet_source_columns_do_not_inherit_generated_metadata_visibility() {
    use datafusion::arrow::array::{RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    let field = Field::new("_metadata", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::from([
            ("paimon.read_files.generated_metadata".into(), "true".into()),
            ("owner".into(), "user".into()),
        ]),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(StringArray::from(vec!["saved user metadata"]))],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let ctx = context(&[("data.parquet", &bytes)]).await;
    assert_eq!(
        columns(
            &ctx,
            "SELECT * FROM read_files('memory://files/data.parquet')"
        )
        .await,
        ["_metadata"]
    );
    assert_eq!(
        columns(
            &ctx,
            "SELECT * FROM read_files('memory://files/data.parquet') f(saved)"
        )
        .await,
        ["saved"]
    );
    let values = rows(
        &ctx,
        "SELECT _metadata FROM read_files('memory://files/data.parquet')",
    )
    .await;
    assert_eq!(
        values[0][0].try_as_str().flatten(),
        Some("saved user metadata")
    );
}

#[tokio::test]
async fn empty_quote_disables_csv_quoting_and_strict_mode_is_explicit() {
    let ctx = context(&[
        ("data.csv", b"\"literal\",tail\n"),
        ("bad.json", b"not JSON\n"),
    ])
    .await;
    let values = rows(&ctx, "SELECT v FROM read_files('memory://files/data.csv', schema => 'v STRING, w STRING', quote => '', mode => 'FAILFAST', schemaEvolutionMode => 'none')").await;
    assert_eq!(values.len(), 1);
    assert_eq!(values[0][0].try_as_str().flatten(), Some("\"literal\""));
    assert!(ctx
        .sql("SELECT * FROM read_files('memory://files/bad.json', mode => 'FAILFAST')")
        .await
        .is_err());
    for options in [
        "mode => 'PERMISSIVE'",
        "mode => 'DROPMALFORMED'",
        "pathGlobFilter => '{a,b}.csv'",
        "schemaEvolutionMode => 'rescue'",
    ] {
        assert!(ctx
            .sql(&format!(
                "SELECT * FROM read_files('memory://files/data.csv', {options})"
            ))
            .await
            .is_err());
    }
}
