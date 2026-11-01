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

use datafusion::common::ScalarValue;
use paimon_datafusion::SQLContext;
#[tokio::test]
async fn lateral_posexplode_outer_preserves_parent_rows_and_zero_based_positions() {
    let ctx = SQLContext::new();
    for (suffix, expected) in [
        ("LIMIT ALL", vec![vec!["1"], vec!["2"]]),
        ("LIMIT (1)", vec![vec!["1"]]),
        ("OFFSET (1)", vec![vec!["2"]]),
    ] {
        assert_eq!(rows(&ctx,&format!("SELECT lv.col FROM (VALUES (ARRAY[1,2])) t(xs) LATERAL VIEW explode(xs) lv {suffix}")).await,expected);
    }
    assert_eq!(rows(&ctx,"SELECT lv.col, row_number() OVER (ORDER BY lv.col) rn FROM (VALUES (ARRAY[1,2])) t(xs) LATERAL VIEW explode(xs) lv QUALIFY rn=1").await,vec![vec!["1","1"]]);
    let batches=ctx.sql("SELECT id, lv.pos, lv.val FROM (VALUES (1, ARRAY[10,20]), (2, ARRAY[]), (3, CAST(NULL AS INT[])), (4, ARRAY[NULL])) t(id,xs) LATERAL VIEW posexplode_outer(xs) lv AS pos, val ORDER BY id,pos").await.unwrap().collect().await.unwrap();
    let rows: Vec<_> = batches
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
        .collect();
    assert_eq!(
        rows,
        vec![
            vec!["1", "0", "10"],
            vec!["1", "1", "20"],
            vec!["2", "NULL", "NULL"],
            vec!["3", "NULL", "NULL"],
            vec!["4", "0", "NULL"]
        ]
    );
}

async fn rows(ctx: &SQLContext, sql: &str) -> Vec<Vec<String>> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|r| {
                (0..b.num_columns())
                    .map(|c| {
                        ScalarValue::try_from_array(b.column(c), r)
                            .unwrap()
                            .to_string()
                    })
                    .collect()
            })
        })
        .collect()
}
#[tokio::test]
async fn lateral_generators_cover_maps_chaining_filters_and_set_operations() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT lv.pos,lv.key,lv.value FROM (SELECT map(ARRAY['a','b'], ARRAY[10,20]) AS m) t LATERAL VIEW posexplode(m) lv ORDER BY lv.pos").await,vec![vec!["0","a","10"],vec!["1","b","20"]]);
    assert_eq!(rows(&ctx,"SELECT lv.key,lv.value FROM (SELECT map(ARRAY['a'],ARRAY[10]) AS m) t LATERAL VIEW explode(m) lv").await,vec![vec!["a","10"]]);
    assert_eq!(rows(&ctx,"SELECT a.x,b.y FROM (VALUES (ARRAY[1,2],ARRAY[3,4])) t(xs,ys) LATERAL VIEW explode(xs) a AS x LATERAL VIEW explode(ys) b AS y WHERE a.x=2 ORDER BY b.y").await,vec![vec!["2","3"],vec!["2","4"]]);
    assert_eq!(rows(&ctx,"SELECT lv.x FROM (VALUES (ARRAY[1,2])) t(xs) LATERAL VIEW explode(xs) lv AS x UNION ALL SELECT 3 ORDER BY x").await,vec![vec!["1"],vec!["2"],vec!["3"]]);
    assert_eq!(
        rows(
            &ctx,
            "SELECT lv.pos,lv.col FROM (VALUES (ARRAY[])) t(xs) LATERAL VIEW posexplode(xs) lv"
        )
        .await,
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT lv.col FROM (VALUES (ARRAY[])) t(xs) LATERAL VIEW OUTER explode(xs) lv"
        )
        .await,
        vec![vec!["NULL"]]
    );
    assert_eq!(rows(&ctx,"SELECT \"L V\".\"Val X\" FROM (VALUES (ARRAY[7])) t(xs) LATERAL VIEW explode(xs) \"L V\" AS \"Val X\"").await,vec![vec!["7"]]);
}
#[tokio::test]
async fn lateral_explode_decodes_dictionary_collections_and_value_nulls() {
    use datafusion::arrow::{
        array::{Array, ArrayRef, DictionaryArray, Int32Array, Int8Array, ListArray, RecordBatch},
        datatypes::{Field, Int32Type, Int8Type, Schema},
    };
    use std::sync::Arc;
    let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(10), Some(20)]),
        None,
        Some(vec![]),
    ]);
    let dict = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(2)]),
            Arc::new(values),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", datafusion::arrow::datatypes::DataType::Int32, false),
            Field::new("xs", dict.data_type().clone(), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            dict,
        ],
    )
    .unwrap();
    let ctx = SQLContext::new();
    ctx.ctx().register_batch("dict", batch).unwrap();
    assert_eq!(rows(&ctx,"SELECT id,lv.pos,lv.col FROM dict LATERAL VIEW posexplode_outer(xs) lv ORDER BY id,lv.pos").await,vec![vec!["1","0","10"],vec!["1","1","20"],vec!["2","NULL","NULL"],vec!["3","NULL","NULL"],vec!["4","NULL","NULL"]]);
}

#[tokio::test]
async fn lateral_expansion_preserves_empty_parents_joins_and_dependent_generators() {
    let ctx = SQLContext::new();
    assert_eq!(rows(&ctx,"SELECT COUNT(*) FROM (SELECT ARRAY[1] xs WHERE FALSE) t LATERAL VIEW explode_outer(xs) lv").await,vec![vec!["0"]]);
    assert_eq!(rows(&ctx,"SELECT a.id,b.id,lv.col FROM (VALUES (1,ARRAY[7,7])) a(id,xs) JOIN (VALUES (1)) b(id) ON a.id=b.id LATERAL VIEW explode(a.xs) lv").await,vec![vec!["1","1","7"],vec!["1","1","7"]]);
    assert_eq!(rows(&ctx,"SELECT b.p,b.v FROM (SELECT ARRAY[ARRAY[1,2],ARRAY[3]] items) t LATERAL VIEW explode(items) a AS xs LATERAL VIEW posexplode(xs) b AS p,v").await,vec![vec!["0","1"],vec!["1","2"],vec!["0","3"]]);
    for sql in [
        "SELECT lv.pos,lv.key,lv.value FROM (SELECT CASE WHEN FALSE THEN map(ARRAY['a'],ARRAY[1]) ELSE NULL END m) t LATERAL VIEW posexplode_outer(m) lv",
        "SELECT lv.pos,lv.key,lv.value FROM (SELECT map(CAST(ARRAY[] AS VARCHAR[]),CAST(ARRAY[] AS INT[])) m) t LATERAL VIEW posexplode_outer(m) lv",
    ] {assert_eq!(rows(&ctx,sql).await,vec![vec!["NULL","NULL","NULL"]]);}
    for sql in [
        "SELECT * FROM (SELECT ARRAY[1] xs) t LATERAL VIEW posexplode(xs) lv AS wrong",
        "SELECT * FROM (SELECT 1 xs) t LATERAL VIEW explode(xs) lv",
        "SELECT * FROM (SELECT ARRAY[1] xs) t LATERAL VIEW unsupported_generator(xs) lv",
    ] {
        assert!(ctx.sql(sql).await.is_err(), "{sql}");
    }
}
