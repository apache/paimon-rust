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
use paimon::{catalog::Identifier, Catalog};

#[tokio::test]
async fn comments_round_trip_without_changing_fields_or_data() {
    let (_tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(catalog.clone()).await;
    ctx.sql("CREATE TABLE default.c (id INT COMMENT 'old') COMMENT 'table ''quote'''")
        .await
        .unwrap();
    ctx.sql("INSERT INTO default.c VALUES (7)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let id = Identifier::new("default", "c");
    let before = catalog.get_table(&id).await.unwrap().schema().clone();
    assert_eq!(before.comment(), Some("table 'quote'"));
    ctx.sql("ALTER TABLE default.c ALTER COLUMN id COMMENT '列 ''quote'''")
        .await
        .unwrap();
    ctx.sql("COMMENT ON TABLE default.c IS 'new'")
        .await
        .unwrap();
    let after = catalog.get_table(&id).await.unwrap().schema().clone();
    assert_eq!(after.comment(), Some("new"));
    assert_eq!(after.fields()[0].description(), Some("列 'quote'"));
    assert_eq!(after.fields()[0].id(), before.fields()[0].id());
    assert_eq!(
        after.fields()[0].data_type(),
        before.fields()[0].data_type()
    );
    ctx.sql("COMMENT ON COLUMN default.c.id IS NULL")
        .await
        .unwrap();
    ctx.sql("COMMENT ON TABLE default.c IS NULL").await.unwrap();
    let cleared = catalog.get_table(&id).await.unwrap().schema().clone();
    assert_eq!(cleared.comment(), None);
    assert_eq!(cleared.fields()[0].description(), Some(""));
    assert_eq!(
        common::collect_id_value(&ctx, "SELECT id, id AS value FROM default.c").await,
        vec![(7, 7)]
    );
}

#[tokio::test]
async fn comments_resolve_names_and_reject_invalid_targets_without_mutation() {
    let (_tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(catalog.clone()).await;
    ctx.sql("CREATE TABLE default.c (id INT) COMMENT 'original'")
        .await
        .unwrap();
    ctx.sql("CREATE TABLE default.\"MixedCase\" (id INT) COMMENT 'upper'")
        .await
        .unwrap();
    ctx.sql("COMMENT ON COLUMN DEFAULT.C.ID IS 'lower column'")
        .await
        .unwrap();
    ctx.sql("COMMENT ON TABLE DEFAULT.C IS 'lower table'")
        .await
        .unwrap();
    assert_eq!(
        catalog
            .get_table(&Identifier::new("default", "c"))
            .await
            .unwrap()
            .schema()
            .comment(),
        Some("lower table")
    );
    assert_eq!(
        catalog
            .get_table(&Identifier::new("default", "MixedCase"))
            .await
            .unwrap()
            .schema()
            .comment(),
        Some("upper")
    );
    ctx.sql("COMMENT ON TABLE default.\"MixedCase\" IS 'quoted'")
        .await
        .unwrap();
    assert_eq!(
        catalog
            .get_table(&Identifier::new("default", "MixedCase"))
            .await
            .unwrap()
            .schema()
            .comment(),
        Some("quoted")
    );
    let before = catalog
        .get_table(&Identifier::new("default", "c"))
        .await
        .unwrap()
        .schema()
        .clone();
    for sql in [
        "COMMENT ON COLUMN default.c.missing IS 'bad'",
        "COMMENT IF EXISTS ON COLUMN default.c.missing IS 'bad'",
        "ALTER TABLE IF EXISTS default.c ALTER COLUMN missing COMMENT 'bad'",
        "ALTER TABLE default.c ALTER COLUMN id COMMENT 'bad'; DROP TABLE default.c",
        "CREATE TABLE default.duplicate(id INT) COMMENT 'a' COMMENT 'b'",
        "COMMENT ON TABLE default.\"c$branch_test\" IS 'bad'",
    ] {
        assert!(ctx.sql(sql).await.is_err(), "{sql}");
        assert_eq!(
            catalog
                .get_table(&Identifier::new("default", "c"))
                .await
                .unwrap()
                .schema()
                .id(),
            before.id(),
            "{sql}"
        );
    }
    ctx.sql("ALTER TABLE IF EXISTS default.missing ALTER COLUMN id COMMENT 'ok'")
        .await
        .unwrap();
    ctx.sql("CREATE TEMPORARY VIEW default.c AS SELECT 1 AS id")
        .await
        .unwrap();
    assert!(ctx
        .sql("COMMENT ON TABLE default.c IS 'hidden mutation'")
        .await
        .is_err());
    assert_eq!(
        catalog
            .get_table(&Identifier::new("default", "c"))
            .await
            .unwrap()
            .schema()
            .comment(),
        Some("lower table")
    );
}
