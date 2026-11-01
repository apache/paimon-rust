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

use crate::table::vector_search_test_utils::{de_vector_table, de_vector_table_with_metric};
use crate::table::{
    SearchScanSplit, SearchTask, SearchTaskExecutor, SearchTaskResult, Table, VectorScanPlan,
};
use std::collections::HashMap;

async fn raw_table() -> Table {
    let table = de_vector_table().await;
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    table
        .copy_with_pinned_snapshot(Some(&snapshot))
        .copy_with_options(HashMap::from([(
            "vector-index.search-mode".into(),
            "full".into(),
        )]))
}

async fn two_vector_table(pk: bool, declaration: &str) -> Table {
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{ArrayType, DataType, FloatType, IntType, Schema, TableSchema, VectorType};
    use crate::table::{TableCommit, TableWrite};
    use arrow_array::types::Float32Type;
    use arrow_array::{ArrayRef, FixedSizeListArray, Int32Array, ListArray, RecordBatch};
    use arrow_schema::{Field, Schema as ArrowSchema};
    use std::sync::Arc;
    let vector_type = if pk {
        DataType::Vector(VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap())
    } else {
        DataType::Array(ArrayType::new(DataType::Float(FloatType::new())))
    };
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("embedding", vector_type.clone())
        .column("other", vector_type)
        .option("write-only", "true")
        .option("vector-index.search-mode", "full")
        .option("ivf-flat.dimension", "3")
        .option("diskann.dimension", "2");
    if pk {
        schema = schema
            .primary_key(["id"])
            .option("bucket", "1")
            .option("deletion-vectors.enabled", "true")
            .option(declaration, "embedding");
        if declaration == "pk-vector.index.columns" {
            schema = schema.option("fields.embedding.pk-vector.index.type", "ivf-flat");
        }
    } else {
        schema = schema
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("global-index.enabled", "true");
    }
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "index_options"),
        format!("memory:/index-options-{}", uuid::Uuid::new_v4()),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>([
        Some(vec![Some(3.0), Some(0.0)]),
        Some(vec![Some(1.0), Some(1.0)]),
    ]);
    let vectors: ArrayRef = if pk {
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("element", arrow_schema::DataType::Float32, true)),
            2,
            vectors.values().clone(),
            None,
        ))
    } else {
        Arc::new(vectors)
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1, 2])),
        vectors.clone(),
        vectors,
    ];
    let arrow = Arc::new(ArrowSchema::new(vec![
        Field::new("id", arrow_schema::DataType::Int32, false),
        Field::new("embedding", columns[1].data_type().clone(), true),
        Field::new("other", columns[2].data_type().clone(), true),
    ]));
    let mut writer = TableWrite::new(&table, "index-options".into()).unwrap();
    writer
        .write_arrow_batch(&RecordBatch::try_new(arrow, columns).unwrap())
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    if pk {
        // Java PK raw fallback reads eligible compacted sources, never L0.
        for file in messages
            .iter_mut()
            .flat_map(|message| &mut message.new_files)
        {
            file.level = 1;
            file.file_source = Some(1);
        }
    }
    TableCommit::new(table.clone(), "index-options".into())
        .commit(messages)
        .await
        .unwrap();
    table
}

fn raw_type(plan: &VectorScanPlan) -> Option<&str> {
    plan.splits().iter().find_map(|source| match source {
        SearchScanSplit::Raw { index_type, .. } => index_type.as_deref(),
        _ => None,
    })
}

struct Worker {
    table: Table,
    options: HashMap<String, String>,
}

#[async_trait::async_trait]
impl SearchTaskExecutor for Worker {
    async fn execute(&self, tasks: Vec<SearchTask>) -> crate::Result<Vec<SearchTaskResult>> {
        let (context, frames) = SearchTask::serialize_batch(&tasks)?;
        let mut results = Vec::new();
        for frame in frames {
            let task = SearchTask::deserialize_with_context(&frame, &context)?;
            let wire: serde_json::Value = serde_json::from_slice(&task.serialize()?).unwrap();
            for query in wire["payload"]["queries"]["queries"].as_array().unwrap() {
                assert!(query.get("index_type").is_none());
                assert_eq!(
                    query["options"],
                    serde_json::to_value(&self.options).unwrap()
                );
            }
            results.push(SearchTaskResult::deserialize(
                &task.execute(&self.table).await?.serialize()?,
            )?);
        }
        results.reverse();
        Ok(results)
    }
}

fn declaration(kind: &str, column: &str) -> HashMap<String, String> {
    HashMap::from([(format!("global-index.{kind}.index-columns"), column.into())])
}

#[tokio::test]
async fn untyped_raw_json_metrics_preserve_source_priority_and_worker_results() {
    let original = raw_table().await;
    let key = "fields.embedding.index-options";
    for mode in ["full", "detail"] {
        for (table_json, query_json, expected) in [
            (None, None, vec![1.0, 0.5, 1.0 / 3.0]),
            (
                Some(r#"{"metric":"cosine"}"#),
                None,
                vec![1.0, 1.0 / 2.0_f32.sqrt(), 0.0],
            ),
            (
                None,
                Some(r#"{"distance.metric":"cosine"}"#),
                vec![1.0, 1.0 / 2.0_f32.sqrt(), 0.0],
            ),
            (
                Some(r#"{"metric":"cosine"}"#),
                Some(r#"{"metric":"l2"}"#),
                vec![1.0, 0.5, 1.0 / 3.0],
            ),
            // An explicit query metric need not parse an unused invalid table JSON.
            (
                Some("invalid unused table JSON"),
                Some(r#"{"metric":"inner_product"}"#),
                vec![1.0, 1.0, 0.0],
            ),
            // Other implementation namespaces in JSON cannot invent a metric/type.
            (
                None,
                Some(r#"{"diskann.metric":"cosine","index_type":"diskann"}"#),
                vec![1.0, 0.5, 1.0 / 3.0],
            ),
        ] {
            let table = original.copy_with_options(
                table_json
                    .map(|json| HashMap::from([(key.into(), json.into())]))
                    .unwrap_or_default(),
            );
            let mut options = HashMap::from([
                ("vector-index.search-mode".into(), mode.into()),
                ("opaque.query.option".into(), r#"{"values":[1,2]}"#.into()),
            ]);
            if let Some(json) = query_json {
                options.insert(key.into(), json.into());
            }
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column("embedding")
                .with_query_vector(vec![1.0, 0.0])
                .with_limit(3)
                .with_options(options.clone());
            let plan = builder.new_scan().unwrap().plan().await.unwrap();
            assert_eq!(raw_type(&plan), None);
            let plan: VectorScanPlan =
                serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
            let reader = builder.new_read().unwrap();
            let local = reader.read(plan.clone()).await.unwrap();
            assert_eq!(local.row_ids().unwrap().scores, expected);
            let worker = Worker {
                table: table.clone(),
                options: options.clone(),
            };
            let remote = reader
                .read_with_executor(plan.clone(), &worker)
                .await
                .unwrap();
            assert_eq!(
                remote.row_ids().unwrap().row_ids,
                local.row_ids().unwrap().row_ids
            );
            assert_eq!(remote.row_ids().unwrap().scores, expected);
            let mut batch = table.new_batch_vector_search_builder();
            batch
                .with_vector_column("embedding")
                .with_query_vectors(vec![vec![1.0, 0.0]; 2])
                .with_limit(3)
                .with_options(options);
            for result in batch
                .new_read()
                .unwrap()
                .read_with_executor(plan, &worker)
                .await
                .unwrap()
            {
                assert_eq!(result.row_ids().unwrap().scores, expected);
            }
        }
    }
}

#[tokio::test]
async fn configuration_is_captured_by_single_batch_plan_reader_and_worker() {
    let table = raw_table().await;
    let mut options = declaration("ivf-flat", "embedding");
    options.insert(
        "fields.embedding.index-options".into(),
        r#"{"dimension":2,"nlist":1}"#.into(),
    );
    options.insert(
        "opaque.future.parameter".into(),
        r#"{"value":[1,2]}"#.into(),
    );
    let mut builder = table.new_vector_search_builder();
    builder
        .with_options(options.clone())
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(3);
    let scan = builder.new_scan().unwrap();
    let reader = builder.new_read().unwrap();
    let changed = HashMap::from([
        ("global-index.ivf-flat.index-columns".into(), "other".into()),
        (
            "global-index.diskann.index-columns".into(),
            "embedding".into(),
        ),
        ("ivf-flat.metric".into(), "l2".into()),
        ("diskann.metric".into(), "cosine".into()),
    ]);
    builder.with_options(changed.clone());
    let plan = scan.plan().await.unwrap();
    assert_eq!(raw_type(&plan), Some("ivf-flat"));
    let restored: VectorScanPlan =
        serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
    let local = reader.read(restored.clone()).await.unwrap();
    assert_eq!(local.row_ids().unwrap().scores, vec![1.0, 1.0, 0.0]);
    // Captured raw split metadata wins over a fresh reader's new declarations.
    let mut fresh = table.new_vector_search_builder();
    fresh
        .with_options(changed)
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(3);
    let result = fresh
        .new_read()
        .unwrap()
        .read(restored.clone())
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().scores, vec![1.0, 0.5, 1.0 / 3.0]);
    let worker = Worker {
        table: table.clone(),
        options: options.clone(),
    };
    let remote = reader.read_with_executor(restored, &worker).await.unwrap();
    assert_eq!(
        remote.row_ids().unwrap().scores,
        local.row_ids().unwrap().scores
    );
    let mut batch = table.new_batch_vector_search_builder();
    batch
        .with_options(options)
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0]; 2])
        .with_limit(3);
    let scan = batch.new_scan().unwrap();
    let reader = batch.new_read().unwrap();
    batch.with_options(declaration("diskann", "embedding"));
    for result in reader
        .read_with_executor(scan.plan().await.unwrap(), &worker)
        .await
        .unwrap()
    {
        assert_eq!(
            result.row_ids().unwrap().scores,
            local.row_ids().unwrap().scores
        );
    }
}

#[tokio::test]
async fn raw_fallback_preserves_unmodified_declarations_and_metric_options() {
    let raw = raw_table().await;
    for declaration in [
        "global-index.ivf-flat.index-columns",
        "global-index.ivf-flat.index-column",
        "pk-vector.index.columns",
    ] {
        let table = raw.copy_with_options(HashMap::from([
            (declaration.into(), "embedding".into()),
            (
                "fields.embedding.pk-vector.index.type".into(),
                "ivf-flat".into(),
            ),
            (
                "fields.embedding.pk-vector.index.options".into(),
                "invalid old JSON".into(),
            ),
            (
                "fields.embedding.index-options".into(),
                r#"{"dimension":2,"nlist":1}"#.into(),
            ),
        ]));
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0, 0.0])
            .with_limit(3);
        assert_eq!(
            raw_type(&builder.new_scan().unwrap().plan().await.unwrap()),
            Some("ivf-flat")
        );
        assert_eq!(
            builder.execute().await.unwrap().row_ids().unwrap().scores,
            vec![1.0, 1.0, 0.0]
        );
        builder.with_options(HashMap::from([
            ("ivf-flat.metric".into(), "cosine".into()),
            ("diskann.metric".into(), "l2".into()),
        ]));
        let result = builder.execute().await.unwrap();
        assert_eq!(result.row_ids().unwrap().row_ids, vec![0, 2, 1]);
        assert!((result.row_ids().unwrap().scores[1] - 1.0_f32 / 2.0_f32.sqrt()).abs() < 1e-6);
    }
}

#[tokio::test]
async fn removed_type_options_do_not_change_raw_defaults() {
    let table = raw_table().await;
    for key in [
        "index_type",
        "index-type",
        "vector.index-type",
        "fields.embedding.index-type",
        "fields.embedding.pk-vector.index.type",
    ] {
        let table = table.copy_with_options(HashMap::from([(key.into(), "diskann".into())]));
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0, 0.0])
            .with_limit(3)
            .with_options(HashMap::from([(key.into(), "ivf-flat".into())]));
        assert_eq!(
            raw_type(&builder.new_scan().unwrap().plan().await.unwrap()),
            None
        );
        let result = builder.execute().await.unwrap();
        assert_eq!(result.row_ids().unwrap().row_ids, vec![0, 2, 1]);
        assert_eq!(result.row_ids().unwrap().scores, vec![1.0, 0.5, 1.0 / 3.0]);
    }
}

#[tokio::test]
async fn metadata_precedes_configuration_even_with_partial_raw_coverage() {
    let original = de_vector_table_with_metric(Some("l2")).await;
    for kind in ["diskann", "invalid-unused-type"] {
        let table = original.copy_with_options(HashMap::from([
            ("vector-index.search-mode".into(), "full".into()),
            (
                format!("global-index.{kind}.index-columns"),
                "embedding".into(),
            ),
            (format!("{kind}.metric"), "invalid-unused-metric".into()),
        ]));
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0, 0.0])
            .with_limit(3);
        let plan = builder.new_scan().unwrap().plan().await.unwrap();
        let mut splits = plan.splits().to_vec();
        splits.push(SearchScanSplit::Raw {
            row_ranges: vec![crate::table::RowRange::new(0, 2)],
            index_type: Some("ivf-flat".into()),
            scalar_index_files: vec![],
        });
        let result = builder
            .new_read()
            .unwrap()
            .read(VectorScanPlan::new(plan.snapshot().cloned(), splits).unwrap())
            .await
            .unwrap();
        assert_eq!(result.row_ids().unwrap().row_ids, vec![0, 2, 1]);
        assert_eq!(result.row_ids().unwrap().scores, vec![1.0, 0.5, 1.0 / 3.0]);
    }
}

#[tokio::test]
async fn pk_raw_fallback_keeps_table_metric_in_single_batch_and_hybrid() {
    use futures::TryStreamExt;
    for declaration in [
        "global-index.ivf-flat.index-columns",
        "global-index.ivf-flat.index-column",
        "pk-vector.index.columns",
    ] {
        let original = two_vector_table(true, declaration).await;
        for table_options in [
            HashMap::new(),
            HashMap::from([(
                "fields.embedding.index-options".into(),
                r#"{"dimension":2,"metric":"cosine"}"#.into(),
            )]),
        ] {
            let cosine = !table_options.is_empty();
            let table = original.copy_with_options(table_options);
            let expected = if cosine { 1.0 } else { 3.0 };
            // Java PK metrics come from the table result, including raw-only plans.
            for query_options in [
                HashMap::from([("ivf-flat.metric".into(), "l2".into())]),
                HashMap::from([(
                    "fields.embedding.index-options".into(),
                    r#"{"metric":"l2"}"#.into(),
                )]),
            ] {
                let mut builder = table.new_vector_search_builder();
                builder
                    .with_options(query_options.clone())
                    .with_vector_column("embedding")
                    .with_query_vector(vec![1.0, 0.0])
                    .with_limit(1);
                assert_eq!(
                    builder.execute().await.unwrap().positions().unwrap()[0].score,
                    expected
                );
                let mut batch = table.new_batch_vector_search_builder();
                batch
                    .with_options(query_options.clone())
                    .with_vector_column("embedding")
                    .with_query_vectors(vec![vec![1.0, 0.0]; 2])
                    .with_limit(1);
                for result in batch.execute().await.unwrap() {
                    assert_eq!(result.positions().unwrap()[0].score, expected);
                }
                let mut hybrid = table.new_hybrid_search_builder();
                hybrid
                    .add_vector_route("embedding", vec![1.0, 0.0], 1, 1.0, query_options)
                    .unwrap()
                    .with_limit(1);
                let batches = hybrid
                    .execute_read()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    1
                );
                assert_eq!(
                    batches[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                        .unwrap()
                        .value(0),
                    1
                );
            }
        }
    }
}

#[test]
fn raw_configuration_keeps_eager_query_validation() {
    let table = crate::table::vector_search_test_utils::pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", "ivf-flat"),
    ]);
    for query in [vec![], vec![f32::NAN, 0.0], vec![0.0, f32::INFINITY]] {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_options(declaration("diskann", "embedding"))
            .with_vector_column("embedding")
            .with_query_vector(query)
            .with_limit(1);
        assert!(builder.new_read().is_err());
    }
}

#[tokio::test]
async fn hybrid_configuration_is_per_column_and_captured_with_each_route() {
    let table = two_vector_table(false, "unused")
        .await
        .copy_with_options(HashMap::from([
            (
                "global-index.ivf-flat.index-columns".into(),
                "embedding".into(),
            ),
            ("global-index.diskann.index-column".into(), "other".into()),
            (
                "fields.embedding.index-options".into(),
                r#"{"dimension":2,"metric":"l2"}"#.into(),
            ),
            (
                "fields.other.index-options".into(),
                r#"{"dimension":2,"metric":"cosine"}"#.into(),
            ),
        ]));
    let mut hybrid = table.new_hybrid_search_builder();
    for field in ["other", "embedding"] {
        hybrid
            .add_vector_route(
                field,
                vec![1.0, 0.0],
                1,
                1.0,
                HashMap::from([("opaque.route.parameter".into(), format!("value-{field}"))]),
            )
            .unwrap();
    }
    hybrid.with_limit(2);
    let routes = hybrid.route_builders().await.unwrap();
    for route in routes {
        let builder = route.vector_search_builder().unwrap();
        let plan = builder.new_scan().unwrap().plan().await.unwrap();
        let (kind, row_id) = if route.route().field_name() == "other" {
            ("diskann", 0)
        } else {
            ("ivf-flat", 1)
        };
        assert_eq!(raw_type(&plan), Some(kind));
        assert_eq!(
            route.execute().await.unwrap().row_ids().unwrap().row_ids,
            vec![row_id]
        );
        let worker = Worker {
            table: table.clone(),
            options: route.route().options().clone(),
        };
        let remote = builder
            .new_read()
            .unwrap()
            .read_with_executor(plan, &worker)
            .await
            .unwrap();
        assert_eq!(remote.row_ids().unwrap().row_ids, vec![row_id]);
    }
}

#[tokio::test]
async fn raw_build_map_preserves_json_precedence_and_serialized_shards() {
    let table = raw_table().await;
    let mut options = table.schema().options().clone();
    options.extend(declaration("ivf-flat", "embedding"));
    options.extend(HashMap::from([
        ("ivf-flat.metric".into(), "l2".into()),
        (
            "fields.embedding.index-options".into(),
            r#"{"dimension":2,"nlist":1,"metric":"cosine"}"#.into(),
        ),
        ("opaque.future.option".into(), "value".into()),
    ]));
    let mut builder = table.new_global_index_build_builder();
    builder
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options);
    let plan = builder.plan().await.unwrap();
    assert_eq!(plan.snapshot_id(), Some(1));
    assert!(!plan.shards().is_empty());
    let worker = table.copy_with_options(HashMap::from([(
        "fields.embedding.index-options".into(),
        "invalid worker JSON".into(),
    )]));
    for shard in plan.shards() {
        let shard: crate::table::GlobalIndexBuildShard =
            serde_json::from_slice(&serde_json::to_vec(&shard).unwrap()).unwrap();
        let messages = worker
            .new_global_index_build_builder()
            .build_shard(&shard)
            .await
            .unwrap();
        assert!(!messages.is_empty());
        for file in messages.iter().flat_map(|message| &message.new_index_files) {
            let meta: HashMap<String, String> = serde_json::from_slice(
                file.global_index_meta
                    .as_ref()
                    .unwrap()
                    .index_meta
                    .as_ref()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(meta["dimension"], "2");
            assert_eq!(meta["nlist"], "1");
            assert_eq!(meta["metric"], "cosine");
        }
    }
    // Qualified typos still fail at the Rust parameter boundary.
    builder.with_options(HashMap::from([("ivf-flat.nlsit".into(), "1".into())]));
    assert!(builder
        .plan()
        .await
        .unwrap_err()
        .to_string()
        .contains("ivf-flat.nlsit"));
    let empty = table.copy_with_pinned_snapshot(None);
    let plan = empty
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .plan()
        .await
        .unwrap();
    assert_eq!(plan.snapshot_id(), None);
    assert!(plan.shards().is_empty());
}
