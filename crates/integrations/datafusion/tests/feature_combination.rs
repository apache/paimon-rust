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
use datafusion::common::ScalarValue;
use paimon::{catalog::Identifier, Catalog};
#[tokio::test]
async fn six_sql_extensions_work_together() {
    let (_tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(catalog.clone()).await;
    ctx.sql("CREATE TABLE default.events (id INT, txt STRING, b VARBINARY) COMMENT 'events'")
        .await
        .unwrap();
    ctx.sql("INSERT INTO default.events VALUES (1,'a,b',CAST('xy' AS BINARY(4))), (2,NULL,CAST(NULL AS BINARY))").await.unwrap().collect().await.unwrap();
    ctx.sql("ALTER TABLE default.events ALTER COLUMN txt COMMENT 'tokens'")
        .await
        .unwrap();
    ctx.sql("CALL sys.create_tag(table => concat('default.','events'), tag => concat('v','1'), snapshot_id => CAST(1+0 AS BIGINT))").await.unwrap();
    let table = catalog
        .get_table(&Identifier::new("default", "events"))
        .await
        .unwrap();
    assert_eq!(table.schema().comment(), Some("events"));
    assert_eq!(table.schema().fields()[1].description(), Some("tokens"));
    assert_eq!(
        table.tag_manager().get("v1").await.unwrap().unwrap().id(),
        1
    );
    let batches=ctx.sql("SELECT id,p.pos,p.val,hex(b),typeof(p.val),size(split(txt,',')),hour(TIME '12:00:00') FROM default.events LATERAL VIEW posexplode_outer(split(txt,',')) p AS pos,val WHERE p.val RLIKE '^a$' OR p.val IS NULL ORDER BY id,p.pos").await.unwrap().collect().await.unwrap();
    let rows = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|r| {
                (0..b.num_columns())
                    .map(|c| {
                        ScalarValue::try_from_array(b.column(c), r)
                            .unwrap()
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            vec!["1", "0", "a", "78790000", "string", "2", "12"],
            vec!["2", "NULL", "NULL", "NULL", "string", "NULL", "12"]
        ]
    );
}
