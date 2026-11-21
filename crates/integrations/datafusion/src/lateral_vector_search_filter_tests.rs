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
use crate::SQLContext;
use datafusion::arrow::array::builder::{Float32Builder, ListBuilder};
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::empty::EmptyExec;
use paimon::catalog::Identifier;
use paimon::io::FileBlockCache;
use paimon::spec::{Datum, IndexManifest, PredicateBuilder};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};

#[derive(Debug, Default)]
struct ReadTrace(Mutex<Vec<String>>);

#[async_trait]
impl FileBlockCache for ReadTrace {
    async fn get(&self, _: &str, _: std::ops::Range<u64>) -> Option<bytes::Bytes> {
        None
    }

    async fn put(&self, path: &str, _: u64, _: bytes::Bytes) {
        // Never cache: a put records a block actually fetched from storage.
        self.0.lock().unwrap().push(path.to_owned());
    }

    async fn invalidate_path(&self, _: &str) {}
    async fn invalidate_prefix(&self, _: &str) {}
}

impl ReadTrace {
    fn reads_of(&self, names: &[String]) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|path| names.iter().any(|name| path.ends_with(name)))
            .count()
    }
}

fn vectors(start: i32, count: i32) -> ListArray {
    let element = Arc::new(Field::new("element", DataType::Float32, true));
    let mut values = ListBuilder::new(Float32Builder::new()).with_field(element);
    for id in start..start + count {
        values.values().append_slice(&[id as f32, 0.0]);
        values.append(true);
    }
    values.finish()
}

async fn filter_fixture(
    indexed: bool,
) -> (
    tempfile::TempDir,
    Table,
    Arc<ReadTrace>,
    Vec<String>,
    Vec<String>,
) {
    let temp = tempfile::tempdir().unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", temp.path().display()),
    );
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let mut context = SQLContext::new();
    context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();
    context
        .sql(
            "CREATE TABLE paimon.default.lateral_filter (id INT, embedding ARRAY<FLOAT>) \
        WITH ('data-evolution.enabled' = 'true', 'row-tracking.enabled' = 'true', \
        'global-index.row-count-per-shard' = '128', 'scalar-index.search-mode' = 'full', \
        'ivf-flat.dimension' = '2', 'ivf-flat.nlist' = '1', 'ivf-flat.metric' = 'l2')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let table = catalog
        .get_table(&Identifier::new("default", "lateral_filter"))
        .await
        .unwrap();
    let mut tail_files = Vec::new();
    for start in [0, 64] {
        let embeddings = vectors(start, 64);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("embedding", embeddings.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(start..start + 64)),
                Arc::new(embeddings),
            ],
        )
        .unwrap();
        let write = table
            .new_write_builder()
            .with_commit_user("lateral-test")
            .unwrap();
        let mut writer = write.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        if start == 64 {
            tail_files.extend(
                messages
                    .iter()
                    .flat_map(|message| &message.new_files)
                    .map(|file| file.data_file_path(&format!("{}/bucket-0", table.location()))),
            );
        }
        write.new_commit().commit(messages).await.unwrap();
    }
    table
        .new_global_index_build_builder()
        .with_index_type("ivf-flat")
        .with_index_column("embedding")
        .execute()
        .await
        .unwrap();
    if indexed {
        table
            .new_global_index_build_builder()
            .with_index_type("btree")
            .with_index_column("id")
            .execute()
            .await
            .unwrap();
    }
    let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
    let scalar_files = IndexManifest::read(
        table.file_io(),
        &format!(
            "{}/manifest/{}",
            table.location(),
            snapshot.index_manifest().unwrap(),
        ),
    )
    .await
    .unwrap()
    .into_iter()
    .filter(|entry| entry.index_file.index_type == "btree")
    .map(|entry| entry.index_file.file_name)
    .collect();
    let trace = Arc::new(ReadTrace::default());
    let file_io = table
        .file_io()
        .clone()
        .with_file_block_cache(trace.clone(), 4096, "*")
        .unwrap();
    let table = Table::new(
        file_io,
        table.identifier().clone(),
        table.location().into(),
        table.schema().clone(),
        None,
    );
    (temp, table, trace, scalar_files, tail_files)
}

fn filtered_exec(table: Table, filter: Predicate) -> (LateralVectorSearchExec, RecordBatch) {
    let query = vectors(0, 1);
    let query_schema = Arc::new(Schema::new(vec![Field::new(
        "query_vector",
        query.data_type().clone(),
        true,
    )]));
    let query_batch = RecordBatch::try_new(query_schema.clone(), vec![Arc::new(query)]).unwrap();
    let target_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let output_schema = Arc::new(Schema::new(vec![
        query_schema.field(0).clone(),
        target_schema.field(0).clone(),
    ]));
    let exec = LateralVectorSearchExec::new(
        Arc::new(EmptyExec::new(query_schema)),
        table,
        target_schema,
        "embedding".into(),
        Arc::new(Column::new("query_vector", 0)),
        1,
        output_schema,
    )
    .with_filter(filter);
    (exec, query_batch)
}

fn result_id(batch: &RecordBatch) -> i32 {
    assert_eq!(batch.num_rows(), 1);
    batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn exact_scalar_index_is_read_once_across_lateral_batches() {
    let (_temp, table, trace, scalar_files, _) = filter_fixture(true).await;
    assert!(!scalar_files.is_empty());
    let filter = PredicateBuilder::new(table.schema().fields())
        .equal("id", Datum::Int(1))
        .unwrap();
    let (exec, batch) = filtered_exec(table, filter);
    let pinned = OnceCell::new();
    assert_eq!(
        result_id(
            &exec
                .process_batch(batch.clone(), Some(&pinned))
                .await
                .unwrap()
        ),
        1
    );
    assert!(pinned.get().unwrap().prepared.is_some());
    let reads = trace.reads_of(&scalar_files);
    assert!(
        reads > 0,
        "the first batch must read the actual BTree payload"
    );
    assert_eq!(
        result_id(&exec.process_batch(batch, Some(&pinned)).await.unwrap()),
        1
    );
    assert_eq!(
        trace.reads_of(&scalar_files),
        reads,
        "the next batch must reuse the exact scalar answer"
    );
}

#[tokio::test]
async fn unindexed_lateral_filter_keeps_candidate_reads_scoped() {
    let (_temp, table, trace, scalar_files, tail_files) = filter_fixture(false).await;
    assert!(scalar_files.is_empty());
    assert!(!tail_files.is_empty());
    for path in &tail_files {
        table.file_io().delete_file(path).await.unwrap();
    }
    let filter = PredicateBuilder::new(table.schema().fields())
        .less_than("id", Datum::Int(100))
        .unwrap();
    let (exec, batch) = filtered_exec(table, filter);
    let pinned = OnceCell::new();
    for _ in 0..2 {
        assert_eq!(
            result_id(
                &exec
                    .process_batch(batch.clone(), Some(&pinned))
                    .await
                    .unwrap()
            ),
            0
        );
    }
    assert!(pinned.get().unwrap().prepared.is_none());
    let tail_names = tail_files
        .iter()
        .map(|path| path.rsplit('/').next().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        trace.reads_of(&tail_names),
        0,
        "unindexed filters must not trigger a full data pre-scan"
    );
}
