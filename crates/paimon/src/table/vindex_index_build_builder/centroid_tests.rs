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
use crate::spec::{Datum, PredicateBuilder};
use crate::table::centroid_index::{self, Descriptor, Role};
use crate::table::{SearchTask, SearchTaskExecutor, SearchTaskResult};

struct Worker<'a>(&'a Table);

#[async_trait::async_trait]
impl SearchTaskExecutor for Worker<'_> {
    async fn execute(&self, tasks: Vec<SearchTask>) -> Result<Vec<SearchTaskResult>> {
        let (context, frames) = SearchTask::serialize_batch(&tasks)?;
        let mut results = Vec::with_capacity(frames.len());
        for frame in frames {
            let bytes = self.0.execute_search_task(&frame, &context).await?;
            results.push(SearchTaskResult::deserialize(&bytes)?);
        }
        Ok(results)
    }
}

async fn active_entries(table: &Table) -> Vec<crate::spec::IndexManifestEntry> {
    let manager = table.snapshot_manager();
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    IndexManifest::read(
        table.file_io(),
        &manager.manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap()
}

fn centroid_options() -> HashMap<String, String> {
    HashMap::from([
        ("ivf-flat.sharding".into(), "centroid".into()),
        ("ivf-flat.centroid.nlist".into(), "2".into()),
        ("ivf-flat.nlist".into(), "1".into()),
        ("ivf-flat.metric".into(), "l2".into()),
    ])
}

#[tokio::test]
async fn centroid_build_replaces_selected_range_files_and_is_queryable() {
    let path = "memory:/centroid_replace";
    let table = vindex_e2e_table(path, "4");
    setup_dirs(table.file_io(), path).await;
    write_vectors(
        &table,
        (0..8).collect(),
        vec![
            vec![0., 0.],
            vec![1., 0.],
            vec![100., 0.],
            vec![101., 0.],
            vec![10., 0.],
            vec![11., 0.],
            vec![200., 0.],
            vec![201., 0.],
        ],
    )
    .await;
    let mut builder = table.new_global_index_build_builder();
    builder
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(HashMap::from([("ivf-flat.metric".into(), "l2".into())]));
    assert_eq!(builder.execute().await.unwrap(), 2);
    let initial = active_entries(&table).await;

    let mut options = centroid_options();
    options.insert("ivf-flat.nlist".into(), "8".into());
    options.insert("ivf-flat.train.sample-ratio".into(), "0.5".into());
    builder.with_options(options);
    let plan = builder.plan().await.unwrap();
    assert_eq!(plan.shards().len(), 2);
    assert_eq!(plan.replacement_messages()[0].deleted_index_files.len(), 2);
    // The transported plan retains its replacement set and worker settings.
    let plan: crate::table::GlobalIndexBuildPlan =
        serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
    let mut messages = plan.replacement_messages();
    for shard in plan.shards() {
        messages.extend(builder.build_shard(&shard).await.unwrap());
    }
    TableCommit::new(table.clone(), "centroid-generation".into())
        .commit_if_latest_snapshot(messages, plan.snapshot_id().unwrap())
        .await
        .unwrap();

    let indexed = active_entries(&table).await;
    let groups = centroid_index::group(&indexed).unwrap();
    assert_eq!(groups.len(), 2);
    assert!(groups
        .iter()
        .all(|group| group.shards.len() == 2 && group.vector_count == 4));
    for shard in groups.iter().flat_map(|group| group.shards.values()) {
        let descriptor = Descriptor::parse(&shard.index_file).unwrap().unwrap();
        let local = centroid_index::payload_options(&descriptor).unwrap();
        assert_eq!(local["localNlist"], "1");
    }
    assert!(initial.iter().all(|old| !indexed.contains(old)));

    let mut search = table.new_batch_vector_search_builder();
    search
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1., 0.], vec![201., 0.]])
        .with_limit(1)
        .with_options(HashMap::from([("centroid.nprobe".into(), "1".into())]));
    let results = search.execute().await.unwrap();
    assert_eq!(results[0].row_ids().unwrap().row_ids, [1]);
    assert_eq!(results[1].row_ids().unwrap().row_ids, [7]);
    assert_eq!(results[1].row_ids().unwrap().scores, [1.0]);

    // A second build atomically replaces the preceding centroid generation.
    assert_eq!(builder.execute().await.unwrap(), 6);
    let replaced = active_entries(&table).await;
    assert_eq!(replaced.len(), indexed.len());
    assert!(replaced.iter().all(|entry| !indexed.contains(entry)));
}

#[tokio::test]
async fn centroid_partition_replacement_requires_valid_matching_partitions() {
    let path = "memory:/centroid_partition_replace";
    let schema = vindex_schema_builder(vindex_e2e_options("8"))
        .column("pt", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .build()
        .unwrap();
    let table = test_table_with_io(FileIOBuilder::new("memory").build().unwrap(), path, schema);
    setup_dirs(table.file_io(), path).await;
    let vectors = build_vector_batch(vec![0, 1, 2, 3], vec![vec![1., 0.]; 4]);
    let mut fields = vectors.schema().fields().to_vec();
    fields.push(Arc::new(ArrowField::new("pt", ArrowDataType::Int32, true)));
    let mut columns = vectors.columns().to_vec();
    columns.push(Arc::new(Int32Array::from(vec![0, 0, 1, 1])));
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
    let mut writer = TableWrite::new(&table, "centroid-partitions".into()).unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    TableCommit::new(table.clone(), "centroid-partitions".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let mut builder = table.new_global_index_build_builder();
    builder
        .with_index_column("embedding")
        .with_index_type("ivf-flat");
    assert_eq!(builder.execute().await.unwrap(), 2);
    let initial = active_entries(&table).await;
    let untouched = initial
        .iter()
        .find(|entry| {
            BinaryRow::from_serialized_bytes(&entry.partition)
                .unwrap()
                .get_int(0)
                .unwrap()
                == 0
        })
        .unwrap();
    let filter = PredicateBuilder::new(table.schema().fields())
        .greater_than("pt", Datum::Int(0))
        .unwrap();
    builder
        .with_options(centroid_options())
        .with_partition_filter(filter)
        .unwrap();
    let plan = builder.plan().await.unwrap();
    assert_eq!(plan.shards().len(), 1);
    let replacements = plan.replacement_messages();
    assert_eq!(replacements.len(), 1);
    assert_eq!(replacements[0].deleted_index_files.len(), 1);
    assert_ne!(replacements[0].partition, untouched.partition);
    assert_eq!(builder.execute().await.unwrap(), 3);
    let mut indexed = active_entries(&table).await;
    assert!(indexed.contains(untouched));
    assert_eq!(centroid_index::group(&indexed).unwrap().len(), 1);

    // An undecodable partition cannot be scheduled for a filtered deletion.
    indexed
        .iter_mut()
        .find(|entry| entry.index_file.file_name == untouched.index_file.file_name)
        .unwrap()
        .partition = vec![0xff, 0x00];
    let manager = table.snapshot_manager();
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    IndexManifest::write(
        table.file_io(),
        &manager.manifest_path(snapshot.index_manifest().unwrap()),
        &indexed,
    )
    .await
    .unwrap();
    let error = builder.plan().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("Invalid index partition for filtered removal"));
}

#[tokio::test]
async fn centroid_query_reads_java_model_and_native_shards_with_rebased_labels() {
    let path = "memory:/centroid_java_fixture";
    let table = vindex_e2e_table(path, "8");
    setup_dirs(table.file_io(), path).await;
    let files: [(&str, &[u8], &[u8], i64); 3] = [
        (
            "router.index",
            include_bytes!("../../../testdata/centroid/router.index"),
            include_bytes!("../../../testdata/centroid/router.meta"),
            0,
        ),
        (
            "shard-0.index",
            include_bytes!("../../../testdata/centroid/shard-0.index"),
            include_bytes!("../../../testdata/centroid/shard-0.meta"),
            2,
        ),
        (
            "shard-1.index",
            include_bytes!("../../../testdata/centroid/shard-1.index"),
            include_bytes!("../../../testdata/centroid/shard-1.meta"),
            2,
        ),
    ];
    write_vectors(
        &table,
        (0..6).collect(),
        vec![
            vec![-5., 0.],
            vec![-2., 0.],
            vec![0., 0.],
            vec![1., 0.],
            vec![100., 0.],
            vec![101., 0.],
        ],
    )
    .await;
    let mut message = CommitMessage::new(BinaryRow::new(0).to_serialized_bytes(), 0, vec![]);
    for (name, bytes, meta, row_count) in files {
        table
            .file_io()
            .new_output(&format!("{path}/index/{name}"))
            .unwrap()
            .write(bytes::Bytes::from_static(bytes))
            .await
            .unwrap();
        message.new_index_files.push(IndexFileMeta {
            index_type: "ivf-flat".into(),
            file_name: name.into(),
            file_size: bytes.len() as i64,
            row_count,
            external_path: None,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 2,
                row_range_end: 5,
                index_field_id: find_index_field(&table, "embedding").unwrap().id(),
                extra_field_ids: None,
                index_meta: Some(meta.to_vec()),
                source_meta: None,
            }),
        });
    }
    let mut partial = message.clone();
    partial.new_index_files.pop();
    let error = TableCommit::new(table.clone(), "centroid-incomplete".into())
        .commit(vec![partial])
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Incomplete or conflicting centroid generation"));
    TableCommit::new(table.clone(), "java-centroid-generation".into())
        .commit(vec![message])
        .await
        .unwrap();
    let indexed = active_entries(&table).await;
    let groups = centroid_index::group(&indexed).unwrap();
    let group = &groups[0];
    assert_eq!(group.descriptor.role, Role::Router(2));
    assert_eq!(
        Descriptor::parse(&group.shards[&0].index_file)
            .unwrap()
            .unwrap()
            .role,
        Role::Shard(0)
    );

    let mut search = table.new_vector_search_builder();
    search
        .with_vector_column("embedding")
        .with_query_vector(vec![100., 0.])
        .with_limit(4)
        .with_options(HashMap::from([("centroid.nprobe".into(), "1".into())]));
    let plan = search.new_scan().unwrap().plan().await.unwrap();
    let result = search
        .new_read()
        .unwrap()
        .read_with_executor(plan, &Worker(&table))
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, [4, 5]);

    // Automatic routing searches both slots for this Top-K.
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![100., 0.])
        .with_limit(4)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, [4, 5, 3, 2]);
}
