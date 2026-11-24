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
#[path = "create_table_as/execution.rs"]
mod execution;
#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use common::{assert_sql_error, create_sql_context, create_test_env, exec, row_count};
use datafusion::datasource::MemTable;
use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::spec::{BigIntType, DataType, IntType, Schema, TableType};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;

async fn query_ids(ctx: &SQLContext, sql: &str) -> Vec<i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .map(Option::unwrap)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn inferred_schema_options_partitioning_and_persistence() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    let source = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int32, false),
        Field::new("part", ArrowType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        source.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow_array::StringArray::from(vec![
                Some("a"),
                None,
                Some("b"),
            ])),
        ],
    )
    .unwrap();
    let partitions = vec![vec![batch.slice(0, 2)], vec![batch.slice(2, 1)]];
    ctx.register_temp_table(
        "source",
        Arc::new(MemTable::try_new(source, partitions).unwrap()),
    )
    .unwrap();
    exec(
        &ctx,
        "/* start */ ; ; -- empty statements
         ; CREATE TABLE result PARTITIONED BY (part)
           WITH ('bucket' = '-1', 'file.format' = 'parquet', 'partition.default-name' = 'it''s null')
           AS SELECT id + 10 AS id, part FROM source WHERE id > 1",
    )
    .await;
    let table = catalog
        .get_table(&Identifier::new("default", "result"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(
        schema.fields()[0].data_type(),
        &DataType::BigInt(BigIntType::with_nullable(false))
    );
    assert!(schema.fields()[1].data_type().is_nullable());
    assert_eq!(schema.partition_keys(), &["part"]);
    assert_eq!(
        schema.options(),
        &HashMap::from([
            ("bucket".into(), "-1".into()),
            ("file.format".into(), "parquet".into()),
            ("partition.default-name".into(), "it's null".into()),
        ]),
        "CTAS must preserve the supplied options without adding internal metadata"
    );
    let definitions = ctx
        .sql("SHOW CREATE TABLE result")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let definition = definitions[0]
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .value(0);
    exec(&ctx, &definition.replace("\"result\"", "\"result_copy\"")).await;
    exec(
        &ctx,
        "CREATE TABLE non_null AS WITH selected AS (SELECT part FROM source) SELECT part FROM selected WHERE part IS NOT NULL",
    )
    .await;
    let non_null = catalog
        .get_table(&Identifier::new("default", "non_null"))
        .await
        .unwrap();
    assert!(non_null.schema().fields()[0].data_type().is_nullable());
    assert_eq!(row_count(&ctx, "SELECT * FROM non_null").await, 2);
    exec(
        &ctx,
        "CREATE TABLE keyed
         TBLPROPERTIES ('bucket' = '2', 'partition' = 'part', 'primary-key' = 'part,id')
         AS SELECT CAST(id AS BIGINT) AS id, part FROM source WHERE part IS NOT NULL",
    )
    .await;
    let keyed = catalog
        .get_table(&Identifier::new("default", "keyed"))
        .await
        .unwrap();
    assert_eq!(keyed.schema().partition_keys(), &["part"]);
    assert_eq!(keyed.schema().primary_keys(), &["part", "id"]);
    let fresh = create_sql_context(catalog).await;
    assert_eq!(query_ids(&fresh, "SELECT id FROM keyed").await, [1, 3]);
    let rows = fresh
        .sql("SELECT id, part FROM result ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        [
            "+----+------+",
            "| id | part |",
            "+----+------+",
            "| 12 |      |",
            "| 13 | b    |",
            "+----+------+"
        ],
        &rows
    );
}

#[tokio::test]
async fn data_evolution_wildcard_omits_virtual_row_id() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(
        &ctx,
        "CREATE TABLE source (id BIGINT) WITH (
            'bucket' = '-1', 'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true'
        )",
    )
    .await;
    exec(&ctx, "INSERT INTO source (id) VALUES (1)").await;
    exec(
        &ctx,
        r#"CREATE TABLE result AS SELECT source.*, source."_ROW_ID" AS source_row_id FROM source"#,
    )
    .await;

    let table = catalog
        .get_table(&Identifier::new("default", "result"))
        .await
        .unwrap();
    assert!(table
        .schema()
        .fields()
        .iter()
        .map(|field| field.name())
        .eq(["id", "source_row_id"]));
    assert_eq!(
        query_ids(&ctx, "SELECT source_row_id FROM result").await,
        [0]
    );
}

#[tokio::test]
async fn empty_results_preserve_schema_and_partitioning() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(
        &ctx,
        "CREATE TABLE IF NOT EXISTS paimon.default.as PARTITIONED BY (id) COMMENT 'query result'
         AS SELECT CAST(NULL AS INT) AS id, CAST(1.25 AS DECIMAL(8, 2)) AS amount WHERE false",
    )
    .await;
    let schema = catalog
        .get_table(&Identifier::new("default", "as"))
        .await
        .unwrap()
        .schema()
        .clone();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.partition_keys(), &["id"]);
    assert_eq!(schema.comment(), Some("query result"));
    assert_eq!(schema.fields()[0].name(), "id");
    assert_eq!(
        schema.fields()[0].data_type(),
        &DataType::Int(IntType::new())
    );
    assert_eq!(schema.fields()[1].name(), "amount");
    let DataType::Decimal(amount) = schema.fields()[1].data_type() else {
        panic!("expected a decimal output");
    };
    assert_eq!((amount.precision(), amount.scale()), (8, 2));
    assert_eq!(row_count(&ctx, "SELECT * FROM paimon.default.as").await, 0);
}

#[tokio::test]
async fn time_precision_loss_is_rejected_before_table_creation() {
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, Time64MicrosecondArray, Time64NanosecondArray};

    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    let nanos = Arc::new(Time64NanosecondArray::from(vec![
        Some(45_296_123_456_789),
        None,
    ]));
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(Time64MicrosecondArray::from(vec![
            Some(45_296_123_456),
            None,
        ])),
        nanos.clone(),
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![Some(0), None]), nanos)
                .unwrap(),
        ),
    ];
    let schema = Arc::new(ArrowSchema::new(
        ["micros", "nanos", "encoded"]
            .into_iter()
            .zip(&columns)
            .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    ctx.register_temp_table(
        "time_source",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    for (index, (query, precision)) in [
        ("SELECT CAST('12:34:56.123456789' AS TIME) AS value", 9),
        ("SELECT micros FROM time_source", 6),
        ("SELECT nanos FROM time_source", 9),
        ("SELECT encoded FROM time_source", 9),
        (
            "SELECT named_struct('value', nanos) AS value FROM time_source",
            9,
        ),
        ("SELECT ARRAY[micros] AS value FROM time_source", 6),
        (
            "SELECT MAP(ARRAY[1], ARRAY[nanos]) AS value FROM time_source",
            9,
        ),
        (
            "SELECT MAP(ARRAY[micros], ARRAY[1]) AS value FROM time_source",
            6,
        ),
        (
            "SELECT named_struct('values', ARRAY[encoded]) AS value FROM time_source",
            9,
        ),
        ("SELECT nanos FROM time_source WHERE false", 9),
    ]
    .iter()
    .enumerate()
    {
        let target = format!("rejected_time_{index}");
        assert_sql_error(
            &ctx,
            &format!("CREATE TABLE {target} AS {query}"),
            &format!("cannot preserve TIME({precision})"),
        )
        .await;
        assert!(
            matches!(
                catalog
                    .get_table(&Identifier::new("default", &target))
                    .await,
                Err(paimon::Error::TableNotExist { .. })
            ),
            "{query}"
        );
    }
}

#[tokio::test]
async fn supported_time_outputs_preserve_values_and_nulls() {
    use arrow_array::{Time32MillisecondArray, Time32SecondArray};

    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(Time32SecondArray::from(vec![Some(45_296), None])),
        Arc::new(Time32MillisecondArray::from(vec![Some(45_296_123), None])),
    ];
    let schema = Arc::new(ArrowSchema::new(
        ["seconds", "millis"]
            .into_iter()
            .zip(&columns)
            .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    ctx.register_temp_table(
        "time_source",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    exec(&ctx, "CREATE TABLE supported_times AS SELECT seconds, millis, named_struct('value', millis) AS nested, ARRAY[seconds] AS items FROM time_source").await;
    exec(&ctx, "CREATE TABLE time_text AS SELECT CAST(CAST('12:34:56.123456789' AS TIME) AS VARCHAR) AS value").await;
    let fresh = create_sql_context(catalog).await;
    let rows = fresh.sql("SELECT seconds, millis, nested.value, items[1] FROM supported_times ORDER BY seconds NULLS LAST")
        .await.unwrap().collect().await.unwrap();
    for (index, expected) in [45_296_000, 45_296_123, 45_296_123, 45_296_000]
        .into_iter()
        .enumerate()
    {
        let values = rows
            .iter()
            .flat_map(|batch| {
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some(expected), None]);
    }
    let rows = fresh
        .sql("SELECT value FROM time_text")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        common::string_value(rows[0].column(0).as_ref(), 0),
        "12:34:56.123456789"
    );
}

#[tokio::test]
async fn quoted_names_and_cross_catalog_target_keep_the_session_source() {
    let (_src, source) = create_test_env();
    let (_dst, destination) = create_test_env();
    let mut ctx = create_sql_context(source).await;
    ctx.register_catalog("Other", destination.clone())
        .await
        .unwrap();
    exec(&ctx, "CREATE DATABASE paimon.current_db").await;
    exec(&ctx, "CREATE DATABASE \"Other\".\"TargetDb\"").await;
    exec(&ctx, "USE paimon.current_db").await;
    exec(
        &ctx,
        "CREATE TABLE \"Source.Table\" AS SELECT 7 AS \"Mixed.Id\"",
    )
    .await;
    exec(
        &ctx,
        "CREATE TABLE \"Other\".\"TargetDb\".\"Source.Table\" AS SELECT 99 AS \"Mixed.Id\"",
    )
    .await;
    exec(
        &ctx,
        r#"CREATE TABLE "Other"."TargetDb"."Result.Table" PARTITIONED BY ("Mixed.Id")
           AS SELECT "Mixed.Id" FROM "Source.Table""#,
    )
    .await;
    assert_eq!(
        query_ids(
            &ctx,
            r#"SELECT "Mixed.Id" FROM "Other"."TargetDb"."Result.Table""#
        )
        .await,
        [7]
    );
    let result = destination
        .get_table(&Identifier::new("TargetDb", "Result.Table"))
        .await
        .unwrap();
    assert_eq!(result.schema().partition_keys(), &["Mixed.Id"]);
    exec(
        &ctx,
        r#"CREATE TABLE "PARTITIONED BY (id)" AS SELECT 2 AS version"#,
    )
    .await;
    exec(
        &ctx,
        r#"CREATE TABLE UPPER_TARGET AS SELECT version AS OF, $$PARTITIONED BY (id)$$ AS value
           FROM "PARTITIONED BY (id)""#,
    )
    .await;
    assert_eq!(
        row_count(
            &ctx,
            "SELECT * FROM upper_target WHERE of = 2 AND value = 'PARTITIONED BY (id)'"
        )
        .await,
        1
    );
    exec(
        &ctx,
        "SET datafusion.sql_parser.enable_ident_normalization = false",
    )
    .await;
    exec(
        &ctx,
        "CREATE TABLE Other.TargetDb.CaseTarget PARTITIONED BY (Part) AS SELECT 8 AS Part",
    )
    .await;
    let table = destination
        .get_table(&Identifier::new("TargetDb", "CaseTarget"))
        .await
        .unwrap();
    assert_eq!(table.schema().partition_keys(), &["Part"]);
    assert_eq!(
        row_count(
            &ctx,
            "SELECT Part FROM Other.TargetDb.CaseTarget WHERE Part = 8"
        )
        .await,
        1
    );
    for (index, selector) in ["VERSION AS OF 1", "TIMESTAMP AS OF '2099-01-01 00:00:00'"]
        .iter()
        .enumerate()
    {
        exec(
            &ctx,
            &format!("CREATE TABLE Other.TargetDb.History{index} AS SELECT Part FROM Other.TargetDb.CaseTarget {selector}"),
        )
        .await;
        assert_eq!(
            row_count(
                &ctx,
                &format!("SELECT Part FROM Other.TargetDb.History{index} WHERE Part = 8")
            )
            .await,
            1
        );
    }
}

#[tokio::test]
async fn time_travel_source_is_materialized_as_a_persistent_table() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(&ctx, "CREATE TABLE \"源表.db.rel.data\" AS SELECT 1 AS id").await;
    catalog
        .create_tag(
            &Identifier::new("default", "源表.db.rel.data"),
            "base",
            Some(1),
            false,
        )
        .await
        .unwrap();
    exec(&ctx, "INSERT INTO \"源表.db.rel.data\" VALUES (2)").await;
    let fresh = create_sql_context(catalog.clone()).await;
    for (table, selector, expected) in [
        ("historical", "VERSION AS OF 1", vec![1]),
        ("tagged", "VERSION AS OF 'base'", vec![1]),
        (
            "at_timestamp",
            "TIMESTAMP AS OF '2099-01-01 00:00:00'",
            vec![1, 2],
        ),
    ] {
        exec(
            &ctx,
            &format!("; CREATE TABLE {table} PARTITIONED BY (id) AS SELECT \"源表.db.rel.data\".id FROM \"源表.db.rel.data\" {selector}"),
        )
        .await;
        let target = catalog
            .get_table(&Identifier::new("default", table))
            .await
            .unwrap();
        assert_eq!(target.schema().partition_keys(), &["id"]);
        assert_eq!(
            query_ids(&fresh, &format!("SELECT * FROM {table}")).await,
            expected
        );
    }
    assert_eq!(
        row_count(&fresh, "SELECT * FROM \"源表.db.rel.data\"").await,
        2
    );
    exec(
        &ctx,
        r#"CREATE TABLE joined AS
           SELECT s1.id, $tag$source VERSION AS OF 99$tag$ AS note
           FROM"源表.db.rel.data" VERSION AS /* tag */ OF 'base' AS s1
           JOIN "源表.db.rel.data" TIMESTAMP AS OF '2099-01-01 00:00:00' AS s2 ON s1.id = s2.id"#,
    )
    .await;
    assert_eq!(
        query_ids(
            &fresh,
            "SELECT id FROM joined WHERE note = 'source VERSION AS OF 99'"
        )
        .await,
        [1]
    );
    exec(
        &ctx,
        r#"CREATE TABLE live_join AS
           SELECT "源表.db.rel.data".id
           FROM "源表.db.rel.data" VERSION AS OF 1
           CROSS JOIN paimon.default."源表.db.rel.data"
           WHERE "源表.db.rel.data".id = 2"#,
    )
    .await;
    assert_eq!(query_ids(&fresh, "SELECT id FROM live_join").await, [2]);
    let query = "AS SELECT id FROM 源表.db.rel.data VERSION AS OF 1";
    assert_sql_error(
        &ctx,
        &format!("CREATE TABLE invalid_name {query}"),
        "Expected 1, 2 or 3 parts, got 4",
    )
    .await;
    assert!(matches!(
        catalog
            .get_table(&Identifier::new("default", "invalid_name"))
            .await,
        Err(paimon::Error::TableNotExist { .. })
    ));
    exec(
        &ctx,
        &format!("CREATE TABLE IF NOT EXISTS historical {query}"),
    )
    .await;
    exec(&ctx, "CREATE DATABASE db2").await;
    exec(&ctx, "CREATE TABLE db2.src AS SELECT 10 AS id").await;
    exec(&ctx, "CREATE TABLE src AS SELECT 12 AS id").await;
    for (table, query) in [
        ("current_join", "SELECT paimon.default.src.id AS a, db2.src.id AS b FROM paimon.default.src CROSS JOIN db2.src"),
        ("historical_join", "SELECT paimon.default.src.id AS a, db2.src.id AS b FROM paimon.default.src VERSION AS OF 1 CROSS JOIN db2.src VERSION AS OF 1"),
        ("qualified_history", "SELECT src.id AS a, paimon.db2.src.id AS b FROM db2.src VERSION AS OF 1 CROSS JOIN src"),
        ("aliased_join", "SELECT a.id AS a, b.id AS b FROM paimon.default.src VERSION AS OF 1 AS a CROSS JOIN db2.src VERSION AS OF 1 AS b"),
    ] {
        exec(&ctx, &format!("CREATE TABLE {table} AS {query}")).await;
        assert_eq!(
            row_count(&fresh, &format!("SELECT * FROM {table} WHERE a = 12 AND b = 10")).await,
            1,
            "{table} from {query}"
        );
    }
    exec(
        &ctx,
        "CREATE TABLE totals AS SELECT paimon.default.src.id + src.id
         FROM paimon.default.src VERSION AS OF 1 CROSS JOIN db2.src VERSION AS OF 1 AS src",
    )
    .await;
    assert_eq!(
        query_ids(
            &fresh,
            r#"SELECT "paimon.default.src.id + src.id" FROM totals"#
        )
        .await,
        [22]
    );
    ctx.ctx().register_catalog(
        "analytics",
        Arc::new(datafusion::catalog::MemoryCatalogProvider::new()),
    );
    exec(&ctx, "SET datafusion.catalog.default_catalog = 'analytics'").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.db2.history AS SELECT id FROM paimon.default.src VERSION AS OF 1",
    )
    .await;
    assert_eq!(query_ids(&fresh, "SELECT * FROM db2.history").await, [12]);
}

#[tokio::test]
async fn ctas_resolves_nested_fields_next_to_snapshot_scans() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(
        &ctx,
        "CREATE TABLE t AS SELECT id FROM (VALUES (1), (2), (3)) AS rows(id)",
    )
    .await;
    let fresh = create_sql_context(catalog).await;

    for (table, snapshot) in [
        ("copied", "t VERSION AS OF 1"),
        ("copied_aliased", "t VERSION AS OF 1 AS s"),
    ] {
        exec(
            &ctx,
            &format!(
                "CREATE TABLE {table} AS SELECT x.t.id AS nested_id FROM {snapshot}
                 CROSS JOIN (SELECT named_struct('id', 999) AS t) AS x"
            ),
        )
        .await;
        assert_eq!(
            query_ids(&fresh, &format!("SELECT nested_id FROM {table}")).await,
            [999, 999, 999],
            "{snapshot}"
        );
    }
}

#[tokio::test]
async fn inferred_nested_types_and_repeated_source_field_ids() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(
        &ctx,
        "CREATE TABLE source AS
         SELECT named_struct('x', 1, 'text', 'hello') AS item, ARRAY[1, 2] AS items,
                MAP(ARRAY['key'], ARRAY[3]) AS entries, CAST(1.25 AS DECIMAL(8, 2)) AS amount,
                TIMESTAMP '2026-11-17 01:02:03' AS ts",
    )
    .await;
    exec(
        &ctx,
        "CREATE TABLE copied AS SELECT item AS first, item AS second, items, entries, amount, ts FROM source",
    )
    .await;
    let schema = catalog
        .get_table(&Identifier::new("default", "copied"))
        .await
        .unwrap()
        .schema()
        .clone();
    assert_ne!(schema.fields()[0].id(), schema.fields()[1].id());
    if let (DataType::Row(first), DataType::Row(second)) = (
        schema.fields()[0].data_type(),
        schema.fields()[1].data_type(),
    ) {
        assert_ne!(first.fields()[0].id(), second.fields()[0].id());
    } else {
        panic!("expected nested ROW types");
    }
    let rows = row_count(
        &ctx,
        "SELECT * FROM copied WHERE first.x = 1 AND second.text = 'hello' AND items[1] = 1
         AND entries['key'] = 3 AND amount = 1.25 AND ts = TIMESTAMP '2026-11-17 01:02:03'",
    )
    .await;
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn inferred_geospatial_types_preserve_crs_and_algorithm() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    let wkb = "decode('0101000000000000000000F03F0000000000000040', 'hex')";
    for (name, predicate, expected_rows) in [("shapes", "true", 1), ("empty_shapes", "false", 0)] {
        exec(
            &ctx,
            &format!(
                "CREATE TABLE {name} AS SELECT
                 CAST({wkb} AS GEOMETRY('EPSG:3857')) AS geom,
                 CAST({wkb} AS GEOGRAPHY('OGC:CRS84', karney)) AS geog
                 WHERE {predicate}"
            ),
        )
        .await;
        let table = catalog
            .get_table(&Identifier::new("default", name))
            .await
            .unwrap();
        assert!(matches!(
            table.schema().fields()[0].data_type(),
            DataType::Geometry(geometry) if geometry.crs() == "EPSG:3857"
        ));
        assert!(matches!(
            table.schema().fields()[1].data_type(),
            DataType::Geography(geography)
                if geography.crs() == "OGC:CRS84" && geography.algorithm().to_string() == "karney"
        ));
        assert_eq!(
            row_count(
                &ctx,
                &format!("SELECT * FROM {name} WHERE geom = {wkb} AND geog = {wkb}")
            )
            .await,
            expected_rows
        );
    }
}

#[tokio::test]
async fn arrow_layouts_are_normalized_for_scalar_and_nested_outputs() {
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::types::Int32Type;
    use arrow_array::LargeStringArray;

    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append("encoded").unwrap();
    dictionary.append_null();
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(dictionary.finish()),
        Arc::new(LargeStringArray::from(vec![Some("large"), None])),
    ];
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("encoded", columns[0].data_type().clone(), true),
        Field::new("large", ArrowType::LargeUtf8, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    ctx.register_temp_table(
        "source",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    exec(&ctx, "CREATE TABLE decoded AS SELECT *, named_struct('encoded', encoded, 'large', large) AS nested, ARRAY[large] AS items FROM source").await;
    assert_eq!(
        row_count(
            &ctx,
            "SELECT * FROM decoded WHERE encoded = 'encoded' AND large = 'large' AND nested.encoded = 'encoded' AND nested.large = 'large' AND items[1] = 'large'"
        )
        .await,
        1
    );
    assert_eq!(
        row_count(
            &ctx,
            "SELECT * FROM decoded WHERE encoded IS NULL AND large IS NULL AND nested.encoded IS NULL AND nested.large IS NULL AND items[1] IS NULL"
        )
        .await,
        1
    );
    use arrow_array::builder::{Int64Builder, MapBuilder, MapFieldNames, StringBuilder};
    use arrow_array::MapArray;
    let names = MapFieldNames {
        entry: "entries".into(),
        key: "key".into(),
        value: "value".into(),
    };
    let mut map = MapBuilder::new(Some(names), StringBuilder::new(), Int64Builder::new());
    map.keys().append_value("discarded");
    map.values().append_value(0);
    map.append(true).unwrap();
    map.keys().append_value("key");
    map.values().append_value(9);
    map.append(true).unwrap();
    map.append(false).unwrap();
    let (field, offsets, entries, nulls, _) = map.finish().into_parts();
    for sorted in [false, true] {
        let map = MapArray::new(
            field.clone(),
            offsets.clone(),
            entries.clone(),
            nulls.clone(),
            sorted,
        )
        .slice(1, 2);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "m",
            map.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(map)]).unwrap();
        ctx.register_temp_table(
            format!("maps_{sorted}"),
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        exec(&ctx, &format!("CREATE TABLE map_copy_{sorted} AS SELECT m, named_struct('m', m) AS nested, ARRAY[m] AS items FROM maps_{sorted}")).await;
        assert_eq!(row_count(&ctx, &format!("SELECT * FROM map_copy_{sorted} WHERE m['key'] = 9 AND nested.m['key'] = 9 AND items[1]['key'] = 9")).await, 1);
        assert_eq!(row_count(&ctx, &format!("SELECT * FROM map_copy_{sorted} WHERE m IS NULL AND nested.m IS NULL AND items[1] IS NULL")).await, 1);
    }
}

#[tokio::test]
async fn existing_target_is_a_noop_without_source_planning_or_execution() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(&ctx, "CREATE TABLE existing AS SELECT 9 AS id").await;
    catalog
        .create_table(
            &Identifier::new("default", "external_target"),
            Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .option("type", TableType::IcebergTable.as_str())
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    exec(
        &ctx,
        "CREATE TABLE object_target (id INT) WITH ('type' = 'object-table', 'path' = 'oss://bucket/objects')",
    )
    .await;
    for target in ["existing", "external_target", "object_target"] {
        let identifier = Identifier::new("default", target);
        let original = catalog.fetch_table_schema(&identifier).await.unwrap();
        for source in [
            "SELECT missing_function(id) FROM missing_source",
            "SELECT * FROM missing_source VERSION AS OF 1",
            "SELECT * FROM missing_source TIMESTAMP AS OF '2099-01-01 00:00:00'",
        ] {
            exec(
                &ctx,
                &format!("; CREATE TABLE IF NOT EXISTS {target} AS {source}"),
            )
            .await;
        }
        assert_sql_error(
            &ctx,
            &format!("CREATE TABLE {target} AS SELECT * FROM missing_source"),
            "already exists",
        )
        .await;
        assert_eq!(
            catalog.fetch_table_schema(&identifier).await.unwrap(),
            original
        );
    }
    assert_eq!(query_ids(&ctx, "SELECT * FROM existing").await, [9]);
}

#[tokio::test]
async fn filesystem_target_lookup_errors_precede_source_planning() {
    let (warehouse, catalog) = create_test_env();
    let ctx = create_sql_context(catalog).await;
    exec(&ctx, "CREATE TABLE broken (id INT)").await;
    std::fs::write(
        warehouse.path().join("default.db/broken/schema/schema-0"),
        b"invalid JSON",
    )
    .unwrap();

    for (target, expected_error) in [
        ("broken", "Failed to parse schema file"),
        ("missing_db.target", "Database missing_db does not exist"),
    ] {
        for clause in ["", "IF NOT EXISTS "] {
            assert_sql_error(
                &ctx,
                &format!("CREATE TABLE {clause}{target} AS SELECT * FROM missing_source"),
                expected_error,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn invalid_ctas_preserves_catalog_and_source_data() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;
    exec(&ctx, "CREATE TABLE source AS SELECT 1 AS id").await;
    ctx.register_temp_table(
        "empty_columns",
        Arc::new(MemTable::try_new(Arc::new(ArrowSchema::empty()), vec![vec![]]).unwrap()),
    )
    .unwrap();
    for sql in [
        "CREATE TABLE bad AUTO_INCREMENT = 42 AS SELECT 1 AS id",
        "CREATE TABLE bad DEFAULT CHARSET = utf8mb4 AS SELECT 1 AS id",
        "CREATE TABLE bad COLLATE = utf8mb4_bin AS SELECT 1 AS id",
        "CREATE TABLE bad WITH ('type' = 'iceberg-table') TBLPROPERTIES ('bucket' = '-1') AS SELECT 1 AS id",
        "CREATE TABLE bad WITH ('bucket' = '0') OPTIONS ('file.format' = 'parquet') AS SELECT 1 AS id",
        "CREATE TABLE bad WITH () OPTIONS ('bucket' = '-1') AS SELECT 1 AS id",
        "CREATE TABLE bad AS SELECT * FROM empty_columns",
        "CREATE TABLE bad AS SELECT NULL AS value",
        "CREATE TABLE bad AS SELECT 1 AS id, 2 AS id",
        "CREATE TABLE bad AS SELECT 1 AS \"_KEY_id\"",
        "CREATE TABLE bad AS SELECT 1 AS alias VERSION AS OF 1",
        "CREATE TABLE bad PARTITIONED BY (missing) AS SELECT 1 AS id",
        r#"CREATE TABLE bad PARTITIONED BY (s.id) AS SELECT 1 AS "s.id""#,
        "CREATE TABLE bad AS SELECT 1 AS id PARTITIONED BY (id)",
        "CREATE TABLE bad WITH ('bucket' = '0') AS SELECT 1 AS id",
        "CREATE EXTERNAL TABLE bad AS SELECT 1 AS id",
        "CREATE TRANSIENT TABLE bad AS SELECT 1 AS id",
        "CREATE GLOBAL TABLE bad AS SELECT 1 AS id",
        "CREATE TABLE bad LOCATION '/tmp/not-a-ctas-table' AS SELECT 1 AS id",
        "CREATE TABLE bad WITH ('type' = 'iceberg-table') AS SELECT 1 AS id",
        "CREATE TABLE bad WITH ('type' = 'format-table') AS SELECT 1 AS id",
        "CREATE TABLE bad (PRIMARY KEY (id)) AS SELECT 1 AS id",
    ] {
        assert!(ctx.sql(sql).await.is_err(), "{sql}");
    }
    for (statement, expected_error) in [
        (
            "CREATE TABLE IF NOT EXISTS source PARTITIONED BY (id) PARTITIONED BY (other) AS SELECT * FROM missing_source",
            "only one PARTITIONED BY clause",
        ),
        (
            "CREATE TABLE bad COMMENT 'first' COMMENT 'second' AS SELECT 1 AS id",
            "Duplicate table COMMENT",
        ),
        (
            r#"CREATE TABLE IF NOT EXISTS "source$branch_dev" AS SELECT * FROM missing_source"#,
            "on Paimon branch 'dev' is not supported",
        ),
        (
            r#"CREATE TABLE "source$snapshots" AS SELECT * FROM missing_source"#,
            "on Paimon system table 'snapshots' is not supported",
        ),
        (
            "CREATE OR REPLACE TABLE source AS SELECT 2 AS id",
            "does not support modifiers before TABLE",
        ),
        (
            "CREATE OR REPLACE TABLE source PARTITIONED BY (id) AS SELECT * FROM missing_source VERSION AS OF 1",
            "does not support modifiers before TABLE",
        ),
        (
            "CREATE OR ALTER TABLE source AS SELECT * FROM source TIMESTAMP AS OF '2099-01-01 00:00:00'",
            "does not support modifiers before TABLE",
        ),
        (
            "CREATE TABLE source (id BIGINT) AS SELECT * FROM source VERSION AS OF 1",
            "explicit column declarations",
        ),
        (
            "CREATE TABLE bad AS SELECT * FROM source VERSION AS OF 1; DROP TABLE source",
            "Expected exactly one SQL statement",
        ),
    ] {
        assert_sql_error(&ctx, &format!("/* start */ ; ; {statement}"), expected_error).await;
    }
    for sql in [
        "; CREATE TABLE bad AS SELECT id INTO side_effect FROM source VERSION AS OF 1",
        "CREATE TABLE bad AS WITH changed AS (SELECT id INTO side_effect FROM source) SELECT * FROM changed",
        "CREATE TABLE bad AS SELECT id FROM source UNION ALL SELECT id INTO side_effect FROM source",
        "CREATE TABLE bad AS INSERT INTO source VALUES (2)",
        "CREATE TABLE bad AS UPDATE source SET id = 2",
        "CREATE TABLE bad AS DELETE FROM source",
        "CREATE TABLE bad AS MERGE INTO source t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE",
        "CREATE TABLE bad AS WITH changed AS (INSERT INTO source VALUES (2) RETURNING id) SELECT * FROM changed",
    ] {
        assert_sql_error(&ctx, sql, "requires a read-only query").await;
    }
    for missing in ["bad", "side_effect"] {
        assert!(matches!(
            catalog
                .get_table(&Identifier::new("default", missing))
                .await,
            Err(paimon::Error::TableNotExist { .. })
        ));
    }
    assert_eq!(query_ids(&ctx, "SELECT id FROM source").await, [1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_ctas_resolves_the_source_only_after_checking_the_target() {
    let tmp = tempfile::tempdir().unwrap();
    let server = mock_server::start_mock_server(
        "ctas".into(),
        tmp.path().to_string_lossy().into_owned(),
        paimon::api::ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".into(),
        )])),
        vec!["source_db".into(), "default".into()],
    )
    .await;
    let mut options = Options::new();
    options.set(CatalogOptions::URI, server.url().unwrap());
    options.set(CatalogOptions::WAREHOUSE, "ctas");
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let source = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let (_dest, destination) = create_test_env();
    let mut ctx = SQLContext::new();
    ctx.register_catalog("source", source).await.unwrap();
    ctx.register_catalog("dest", destination).await.unwrap();
    exec(&ctx, "USE source.source_db").await;
    exec(
        &ctx,
        "CREATE FUNCTION plus_one(x BIGINT) RETURNS BIGINT RETURN x + 1",
    )
    .await;
    exec(&ctx, "CREATE TABLE inputs AS SELECT 8 AS id").await;
    let query = "SELECT plus_one(id) AS id FROM inputs";
    server.add_table_with_schema(
        "default",
        "objects",
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("type", TableType::ObjectTable.as_str())
            .build()
            .unwrap(),
        "oss://bucket/objects",
    );
    exec(
        &ctx,
        "CREATE TABLE IF NOT EXISTS source.default.objects AS SELECT * FROM missing_source",
    )
    .await;
    assert_sql_error(
        &ctx,
        "CREATE TABLE source.default.objects AS SELECT * FROM missing_source",
        "already exists",
    )
    .await;
    assert_eq!(
        server.table_get_call_count("source_db", "missing_source"),
        0
    );
    server.add_no_permission_table("default", "forbidden");
    for (target, expected_error) in [
        (
            "source.missing_db.result",
            "Database missing_db does not exist",
        ),
        ("source.default.forbidden", "No Permission"),
        (
            "source.default.\".\"",
            "table name must not be a dot path segment",
        ),
        (
            "source.\"..\".result",
            "database name must not be a dot path segment",
        ),
    ] {
        let before = server.table_get_call_count("source_db", "inputs");
        for clause in ["", "IF NOT EXISTS "] {
            assert_sql_error(
                &ctx,
                &format!("CREATE TABLE {clause}{target} AS {query}"),
                expected_error,
            )
            .await;
        }
        assert_eq!(server.table_get_call_count("source_db", "inputs"), before);
    }
    for target in ["dest.default.result", "source.default.result"] {
        let before = server.table_get_call_count("source_db", "inputs");
        exec(
            &ctx,
            &format!("CREATE TABLE {target} WITH ('commit.max-retries' = '0') AS {query}"),
        )
        .await;
        assert_eq!(
            server.table_get_call_count("source_db", "inputs") - before,
            1
        );
        assert_eq!(
            query_ids(&ctx, &format!("SELECT * FROM {target}")).await,
            [9]
        );
    }
}
