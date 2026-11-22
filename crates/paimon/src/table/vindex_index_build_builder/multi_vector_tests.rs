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
use crate::spec::VectorType;
use crate::table::{SearchTask, SearchTaskExecutor, SearchTaskResult};
use arrow_array::Array;

struct Worker<'a>(&'a Table);

#[async_trait::async_trait]
impl SearchTaskExecutor for Worker<'_> {
    async fn execute(&self, tasks: Vec<SearchTask>) -> Result<Vec<SearchTaskResult>> {
        let (context, frames) = SearchTask::serialize_batch(&tasks)?;
        let mut results = vec![];
        for frame in frames {
            let result = self.0.execute_search_task(&frame, &context).await?;
            results.push(SearchTaskResult::deserialize(&result)?);
        }
        Ok(results)
    }
}

fn list_schema(fixed: bool) -> Schema {
    let float = DataType::Float(FloatType::new());
    let embedding = if fixed {
        DataType::Vector(VectorType::new(2, float).unwrap())
    } else {
        DataType::Array(ArrayType::new(float))
    };
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("chunks", DataType::Array(ArrayType::new(embedding)))
        .options(table_options("5"))
        .option("write-only", "true")
        .build()
        .unwrap()
}

fn append_value(builder: &mut Float32Builder, coordinates: [f32; 2]) {
    for coordinate in coordinates {
        builder.append_value(coordinate);
    }
}

type EmbeddingList = Option<Vec<Option<[f32; 2]>>>;

async fn append_lists(table: &Table, first_id: i32, fixed: bool, rows: &[EmbeddingList]) {
    let column: ArrayRef = if fixed {
        let mut outer = ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), 2));
        for row in rows {
            if let Some(embeddings) = row {
                for embedding in embeddings {
                    // Arrow requires two placeholder coordinates for a null fixed-size list.
                    append_value(outer.values().values(), embedding.unwrap_or([0.0, 0.0]));
                    outer.values().append(embedding.is_some());
                }
            }
            outer.append(row.is_some());
        }
        Arc::new(outer.finish())
    } else {
        let mut outer = ListBuilder::new(ListBuilder::new(Float32Builder::new()));
        for row in rows {
            if let Some(embeddings) = row {
                for embedding in embeddings {
                    if let Some(coordinates) = embedding {
                        append_value(outer.values().values(), *coordinates);
                    }
                    outer.values().append(embedding.is_some());
                }
            }
            outer.append(row.is_some());
        }
        Arc::new(outer.finish())
    };
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("chunks", column.data_type().clone(), true),
    ]));
    let ids = Int32Array::from((first_id..first_id + rows.len() as i32).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), column]).unwrap();
    let mut writer = TableWrite::new(table, format!("lists-{first_id}")).unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    TableCommit::new(table.clone(), format!("lists-{first_id}"))
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn multi_vector_indexes_rank_distinct_rows_and_rerank_the_full_candidate_union() {
    for fixed in [false, true] {
        let path = format!("memory:/multivector_candidates_{fixed}");
        let io = FileIOBuilder::new("memory").build().unwrap();
        let table = test_table_with_io(io, &path, list_schema(fixed));
        setup_dirs(table.file_io(), &path).await;
        append_lists(
            &table,
            0,
            fixed,
            &[
                Some(vec![Some([1.0, 0.0]); 24]),
                Some(vec![Some([0.8, 0.6])]),
                Some(vec![Some([0.0, 1.0])]),
                None,
                Some(vec![None]),
                Some(vec![Some([0.0, 1.0]), None, Some([1.0, 0.0])]),
                Some(vec![]),
            ],
        )
        .await;
        let mut build = table.new_global_index_build_builder();
        let mut options = HashMap::from([("ivf-flat.nlist".into(), "1".into())]);
        if !fixed {
            options.insert("ivf-flat.dimension".into(), "2".into());
        }
        build
            .with_index_column("chunks")
            .with_index_type("ivf-flat")
            .with_options(options);
        assert_eq!(build.execute().await.unwrap(), 2);

        let mut search = table.new_vector_search_builder();
        let result = search
            .with_vector_column("chunks")
            .with_query_vector(vec![1.0, 0.0])
            .with_limit(3)
            .execute()
            .await
            .unwrap();
        assert_eq!(result.row_ids().unwrap().row_ids, [0, 5, 1]);

        search
            .with_multi_vector(vec![vec![f32::MAX, 0.0], vec![0.0, f32::MIN_POSITIVE]])
            .with_limit(3);
        let plan = search.new_scan().unwrap().plan().await.unwrap();
        let result = search
            .new_read()
            .unwrap()
            .read_with_executor(plan, &Worker(&table))
            .await
            .unwrap();
        let scores = result.row_ids().unwrap();
        assert_eq!(scores.row_ids, [5, 1, 0]);
        assert_eq!(scores.scores, [2.0, 1.4, 1.0]);

        // DETAIL adds a later row with no ANN file and uses the same matrix scorer.
        append_lists(
            &table,
            7,
            fixed,
            &[Some(vec![Some([1.0, 1.0])]), None, Some(vec![])],
        )
        .await;
        search.with_options(HashMap::from([(
            "vector-index.search-mode".into(),
            "detail".into(),
        )]));
        assert_eq!(
            search.execute().await.unwrap().row_ids().unwrap().row_ids,
            [5, 7, 1]
        );
    }
}

#[tokio::test]
async fn multi_vector_build_and_query_reject_invalid_options_and_shapes() {
    let table = test_table_with_io(
        FileIOBuilder::new("memory").build().unwrap(),
        "memory:/multivector_invalid",
        list_schema(false),
    );
    let mut build = table.new_global_index_build_builder();
    build
        .with_index_type("ivf-flat")
        .with_index_column("chunks");
    assert!(build
        .plan()
        .await
        .unwrap_err()
        .to_string()
        .contains("dimension option"));
    build.with_options(HashMap::from([
        ("ivf-flat.dimension".into(), "2".into()),
        ("ivf-flat.metric".into(), "l2".into()),
    ]));
    assert!(build
        .plan()
        .await
        .unwrap_err()
        .to_string()
        .contains("cosine similarity"));
    build.with_options(HashMap::from([
        ("ivf-flat.dimension".into(), "2".into()),
        ("ivf-flat.sharding".into(), "centroid".into()),
    ]));
    assert!(build
        .plan()
        .await
        .unwrap_err()
        .to_string()
        .contains("range sharding"));

    let mut search = table.new_vector_search_builder();
    search.with_vector_column("chunks").with_limit(1);
    for query in [
        vec![],
        vec![vec![1.0], vec![1.0, 2.0]],
        vec![vec![f32::NAN, 0.0]],
    ] {
        search.with_multi_vector(query);
        assert!(search.new_read().is_err());
    }
}
