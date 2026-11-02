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
async fn call_folds_typed_constant_arguments_before_changing_metadata() {
    let (_tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(catalog.clone()).await;
    ctx.sql("CREATE TABLE default.c (id INT)").await.unwrap();
    ctx.sql("INSERT INTO default.c VALUES (1)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("CALL sys.create_tag(table => concat('default.', 'c'), tag => concat('v', '1'), snapshot_id => CAST(1 + 0 AS BIGINT))").await.unwrap();
    let table = catalog
        .get_table(&Identifier::new("default", "c"))
        .await
        .unwrap();
    assert_eq!(
        table.tag_manager().get("v1").await.unwrap().unwrap().id(),
        1
    );
    for expression in [
        "CAST(NULL AS BIGINT)",
        "random()",
        "(SELECT 1)",
        "missing_column",
    ] {
        assert!(ctx.sql(&format!("CALL sys.create_tag(table => 'default.c', tag => 'bad', snapshot_id => {expression})")).await.is_err(),"{expression}");
        assert!(table.tag_manager().get("bad").await.unwrap().is_none());
    }
}

#[tokio::test]
async fn call_folds_boolean_predicates_and_rejects_errors_before_mutation() {
    let (_tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(catalog.clone()).await;
    ctx.sql("CREATE TABLE default.c (id INT)").await.unwrap();
    ctx.sql("INSERT INTO default.c VALUES (1)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for _ in 0..2 {
        ctx.sql("CALL sys.create_branch(table => 'default.c', branch => CASE WHEN 'prod' LIKE 'p%' THEN 'release' ELSE 'wrong' END, ignore_if_exists => (1=1) IS TRUE)").await.unwrap();
    }
    let table = catalog
        .get_table(&Identifier::new("default", "c"))
        .await
        .unwrap();
    let before = table.tag_manager().list_all().await.unwrap().len();
    for expression in [
        "CAST('bad' AS BIGINT)",
        "1 / 0",
        "ARRAY[1,2]",
        "current_date",
        "CAST(NULL AS VARCHAR)",
    ] {
        assert!(ctx.sql(&format!("CALL sys.create_tag(table => 'default.c', tag => 'bad', snapshot_id => {expression})")).await.is_err(),"{expression}");
        assert_eq!(table.tag_manager().list_all().await.unwrap().len(), before);
    }
    assert!(ctx.sql("CALL sys.create_tag(table => 'default.c', tag => 'bad', snaphot_id => CAST(1 AS BIGINT))").await.is_err());
    assert!(table.tag_manager().get("bad").await.unwrap().is_none());
}
