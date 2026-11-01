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
            .option(declaration, "embedding")
            .option("fields.embedding.pk-vector.index.type", "ivf-flat");
    } else {
        schema = schema
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("global-index.enabled", "true");
    }
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "native_type"),
        format!("memory:/native-type-{}", uuid::Uuid::new_v4()),
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
    let mut writer = TableWrite::new(&table, "native-type".into()).unwrap();
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
    TableCommit::new(table.clone(), "native-type".into())
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
    expected_type: String,
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
                assert_eq!(query["index_type"], self.expected_type);
                assert!(query["options"].get("index_type").is_none());
                assert!(query["options"].get("metric").is_none());
            }
            results.push(SearchTaskResult::deserialize(
                &task.execute(&self.table).await?.serialize()?,
            )?);
        }
        results.reverse();
        Ok(results)
    }
}

#[tokio::test]
async fn native_type_is_captured_by_single_batch_plan_reader_and_worker() {
    let table = raw_table().await;
    let mut builder = table.new_vector_search_builder();
    // Store before configuring a query; unrelated columns and superseded hints
    // cannot influence the selected vector column.
    builder
        .with_vector_index_type("other", "invalid")
        .with_vector_index_type("embedding", "invalid")
        .with_vector_index_type("embedding", "ivf-flat")
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(3);
    let scan = builder.new_scan().unwrap();
    let reader = builder.new_read().unwrap();
    builder.with_vector_index_type("embedding", "diskann");
    let plan = scan.plan().await.unwrap();
    assert_eq!(raw_type(&plan), Some("ivf-flat"));
    let restored: VectorScanPlan =
        serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
    let local = reader.read(restored.clone()).await.unwrap();
    assert_eq!(local.row_ids().unwrap().scores, vec![1.0, 1.0, 0.0]);
    let mut fresh = table.new_vector_search_builder();
    fresh
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(3);
    let result = fresh
        .new_read()
        .unwrap()
        .read(restored.clone())
        .await
        .unwrap();
    assert_eq!(
        result.row_ids().unwrap().scores,
        local.row_ids().unwrap().scores
    );
    // A transported raw source remains authoritative even if a fresh reader's
    // fallback hint names a different parameter namespace.
    fresh
        .with_vector_index_type("embedding", "diskann")
        .with_options(HashMap::from([
            ("ivf-flat.metric".into(), "l2".into()),
            ("diskann.metric".into(), "cosine".into()),
        ]));
    let result = fresh
        .new_read()
        .unwrap()
        .read(restored.clone())
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().scores, vec![1.0, 0.5, 1.0 / 3.0]);
    let worker = Worker {
        table: table.clone(),
        expected_type: "ivf-flat".into(),
    };
    let remote = reader.read_with_executor(restored, &worker).await.unwrap();
    assert_eq!(
        remote.row_ids().unwrap().row_ids,
        local.row_ids().unwrap().row_ids
    );
    assert_eq!(
        remote.row_ids().unwrap().scores,
        local.row_ids().unwrap().scores
    );

    let mut batch = table.new_batch_vector_search_builder();
    batch
        .with_vector_index_type("embedding", "ivf-flat")
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1.0, 0.0]; 2])
        .with_limit(3);
    let scan = batch.new_scan().unwrap();
    let reader = batch.new_read().unwrap();
    batch.with_vector_index_type("embedding", "diskann");
    let results = reader
        .read_with_executor(scan.plan().await.unwrap(), &worker)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    for result in results {
        assert_eq!(
            result.row_ids().unwrap().scores,
            local.row_ids().unwrap().scores
        );
    }
}

#[tokio::test]
async fn native_type_raw_fallback_preserves_declarations_and_metric_options() {
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
        builder
            .with_vector_index_type("embedding", "diskann")
            .with_options(HashMap::from([
                ("ivf-flat.metric".into(), "l2".into()),
                ("diskann.metric".into(), "cosine".into()),
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
async fn metadata_precedes_native_type_even_with_partial_raw_coverage() {
    let table = de_vector_table_with_metric(Some("l2"))
        .await
        .copy_with_options(HashMap::from([
            ("vector-index.search-mode".into(), "full".into()),
            ("diskann.metric".into(), "cosine".into()),
        ]));
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(3)
        .with_options(HashMap::from([("ivf.nprobe".into(), "2".into())]));
    let expected = builder.execute().await.unwrap();
    assert_eq!(
        expected.row_ids().unwrap().scores,
        vec![1.0, 0.5, 1.0 / 3.0]
    );
    for hint in ["diskann", "invalid-unused-type"] {
        builder.with_vector_index_type("embedding", hint);
        let plan = builder.new_scan().unwrap().plan().await.unwrap();
        let result = builder
            .new_read()
            .unwrap()
            .read(plan.clone())
            .await
            .unwrap();
        assert_eq!(
            result.row_ids().unwrap().scores,
            expected.row_ids().unwrap().scores
        );
        // Keep one authoritative indexed source and add raw coverage: the raw
        // range must use persisted semantics too, without interpreting the hint.
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
        assert_eq!(
            result.row_ids().unwrap().row_ids,
            expected.row_ids().unwrap().row_ids
        );
        assert_eq!(
            result.row_ids().unwrap().scores,
            expected.row_ids().unwrap().scores
        );
    }
}

#[tokio::test]
async fn native_type_pk_raw_fallback_uses_its_dimension_metric_and_hybrid_route() {
    use futures::TryStreamExt;
    for declaration in [
        "global-index.ivf-flat.index-columns",
        "global-index.ivf-flat.index-column",
        "pk-vector.index.columns",
    ] {
        let table = two_vector_table(true, declaration).await;
        for query in [
            vec![],
            vec![1.0],
            vec![f32::NAN, 0.0],
            vec![0.0, f32::INFINITY],
        ] {
            let mut invalid = table.new_vector_search_builder();
            invalid
                .with_vector_index_type("embedding", "diskann")
                .with_vector_column("embedding")
                .with_query_vector(query)
                .with_limit(1);
            assert!(
                invalid.new_read().is_err(),
                "fixed vector errors must precede planning"
            );
        }
        let options = HashMap::from([("diskann.metric".into(), "cosine".into())]);
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_index_type("embedding", "diskann")
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0, 0.0])
            .with_limit(1);
        let plan = builder.new_scan().unwrap().plan().await.unwrap();
        assert!(!plan.splits().is_empty());
        assert!(plan.splits().iter().all(|source| matches!(source, SearchScanSplit::PrimaryKey { payloads, .. } if payloads.is_empty())));
        let result = builder
            .new_read()
            .unwrap()
            .read(plan.clone())
            .await
            .unwrap();
        assert_eq!(result.positions().unwrap().len(), 1);
        assert_eq!(result.positions().unwrap()[0].score, 3.0);
        builder.with_options(options.clone());
        let result = builder.new_read().unwrap().read(plan).await.unwrap();
        assert_eq!(result.positions().unwrap().len(), 1);
        assert_eq!(result.positions().unwrap()[0].score, 1.0);
        let mut batch = table.new_batch_vector_search_builder();
        batch
            .with_vector_index_type("embedding", "diskann")
            .with_vector_column("embedding")
            .with_query_vectors(vec![vec![1.0, 0.0]; 2])
            .with_limit(1)
            .with_options(options.clone());
        for result in batch.execute().await.unwrap() {
            assert_eq!(result.positions().unwrap()[0].score, 1.0);
        }
        let mut hybrid = table.new_hybrid_search_builder();
        hybrid.with_vector_index_type("embedding", "diskann");
        hybrid
            .add_vector_route("embedding", vec![1.0, 0.0], 1, 1.0, options)
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

#[test]
fn native_type_does_not_defer_array_query_emptiness_or_finiteness() {
    let table = crate::table::vector_search_test_utils::pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", "ivf-flat"),
    ]);
    for query in [vec![], vec![f32::NAN, 0.0], vec![0.0, f32::INFINITY]] {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_index_type("embedding", "diskann")
            .with_vector_column("embedding")
            .with_query_vector(query)
            .with_limit(1);
        assert!(builder.new_read().is_err());
    }
}

#[tokio::test]
async fn native_type_hybrid_is_per_column_and_captured_before_builder_changes() {
    let table = two_vector_table(false, "unused").await;
    let options = HashMap::from([
        ("ivf-flat.metric".into(), "l2".into()),
        ("diskann.metric".into(), "cosine".into()),
    ]);
    let mut hybrid = table.new_hybrid_search_builder();
    hybrid
        .with_vector_index_type("embedding", "ivf-flat")
        .with_vector_index_type("other", "diskann")
        .with_vector_index_type("embedding", "diskann");
    hybrid
        .add_vector_route("other", vec![1.0, 0.0], 1, 1.0, options.clone())
        .unwrap();
    hybrid
        .add_vector_route("embedding", vec![1.0, 0.0], 1, 1.0, options)
        .unwrap()
        .with_limit(2);
    // Give embedding a different implementation, independent of route order.
    hybrid.with_vector_index_type("embedding", "ivf-flat");
    let routes = hybrid.route_builders().await.unwrap();
    hybrid
        .with_vector_index_type("embedding", "diskann")
        .with_vector_index_type("other", "ivf-flat");
    for route in routes {
        let builder = route.vector_search_builder().unwrap();
        let plan = builder.new_scan().unwrap().plan().await.unwrap();
        let (kind, row_id) = if route.route().field_name() == "other" {
            ("diskann", 0)
        } else {
            ("ivf-flat", 1)
        };
        assert_eq!(raw_type(&plan), Some(kind));
        let result = route.execute().await.unwrap();
        assert_eq!(result.row_ids().unwrap().row_ids, vec![row_id]);
        let worker = Worker {
            table: table.clone(),
            expected_type: kind.into(),
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
