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

use super::*;
use crate::spec::{ArrayType, FloatType, SchemaChange};
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::ArrayRef;

async fn vector_messages(table: &Table, id: i32, vector: &[f32]) -> Vec<CommitMessage> {
    let mut vectors = ListBuilder::new(Float32Builder::new());
    vectors.values().append_slice(vector);
    vectors.append(true);
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        ("embedding", Arc::new(vectors.finish()) as ArrayRef),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

#[tokio::test]
async fn vector_dimension_change_cannot_publish_incompatible_leaf_indexes() {
    let schema = TableSchemaBuilder::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("global-index.enabled", "true")
        .option("global-index.row-count-per-shard", "64")
        .option("global-index.vector.index-columns", "embedding")
        .option(
            "fields.embedding.global-index.vector.index-type",
            "ivf-flat",
        )
        .option(
            "fields.embedding.global-index.vector.index-options",
            r#"{"dimension":2,"nlist":1}"#,
        )
        .option("compaction.min.file-num", "1000")
        .build()
        .unwrap();
    let (table, _) =
        crate::table::maintenance_test_utils::table(TableSchema::new(0, &schema)).await;
    TableCommit::new(table.clone(), "two-dimensional-source".into())
        .commit(vector_messages(&table, 1, &[1., 0.]).await)
        .await
        .unwrap();
    let indexed = latest(&table).await;
    let original = indexes(&table).await;
    assert_eq!(original.len(), 1);
    let previous = &original[0].index_file;
    let original_path = previous
        .external_path
        .clone()
        .unwrap_or_else(|| format!("{}/index/{}", table.location(), previous.file_name));
    let original_bytes = table
        .file_io()
        .new_input(&original_path)
        .unwrap()
        .read()
        .await
        .unwrap();

    // ARRAY<FLOAT> permits different lengths without a schema type change.
    // Two runs are below the default merge trigger, but their dimensions are
    // still incompatible even when algorithm, metric and shard remain equal.
    let evolved = table
        .schema()
        .apply_changes(vec![SchemaChange::set_option(
            "fields.embedding.global-index.vector.index-options".into(),
            r#"{"dimension":3,"nlist":1}"#.into(),
        )])
        .unwrap();
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(evolved.id()))
        .unwrap()
        .write(serde_json::to_vec(&evolved).unwrap().into())
        .await
        .unwrap();
    let current = table.copy_with_resolved_schema(evolved, "main").unwrap();
    let input = vector_messages(&current, 2, &[0., 1., 0.]).await;
    let commit = TableCommit::new(current.clone(), "changed-vector-dimension".into());
    let error = commit
        .filter_and_commit_with_identifier(input.clone(), 77)
        .await
        .expect_err("dimension changes cannot silently retain an incompatible old leaf");
    assert!(error.to_string().contains("dimension"), "{error}");

    let committed_data = latest(&current).await;
    assert_eq!(committed_data.id(), indexed.id() + 1);
    assert_eq!(*committed_data.commit_kind(), CommitKind::APPEND);
    assert_eq!(committed_data.next_row_id(), Some(2));
    assert_eq!(indexes(&current).await, original);
    assert_eq!(
        current
            .file_io()
            .new_input(&original_path)
            .unwrap()
            .read()
            .await
            .unwrap(),
        original_bytes
    );

    for _ in 0..2 {
        let error = commit
            .filter_and_commit_with_identifier(input.clone(), 77)
            .await
            .expect_err("recovery must retry maintenance without publishing mixed dimensions");
        assert!(error.to_string().contains("dimension"), "{error}");
        let recovered = latest(&current).await;
        assert_eq!(recovered.id(), committed_data.id());
        assert_eq!(recovered.next_row_id(), Some(2));
        assert_eq!(indexes(&current).await, original);
        assert!(current.file_io().exists(&original_path).await.unwrap());
    }
}
