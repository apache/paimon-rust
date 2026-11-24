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
use paimon_vindex_core::distance::MetricType;
use std::fs;

#[path = "../../../testdata/centroid/mod.rs"]
mod java_fixture;

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
            vec![2., 0.],
            vec![3., 0.],
            vec![100., 0.],
            vec![101., 0.],
            vec![102., 0.],
            vec![103., 0.],
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
    assert_eq!(plan.shards().len(), 1);
    assert_eq!(plan.shards()[0].row_range(), RowRange::new(0, 7));
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
    assert_eq!(groups.len(), 1);
    let router_meta = groups[0]
        .router
        .index_file
        .global_index_meta
        .as_ref()
        .unwrap();
    assert_eq!(
        router_meta.index_meta.as_deref().unwrap(),
        b"PAIMCENT\x01R\0\0\0\x02l2"
    );
    assert!(groups
        .iter()
        .all(|group| group.shards.len() == 2 && group.vector_count == 8));
    for shard in groups.iter().flat_map(|group| group.shards.values()) {
        let descriptor = Descriptor::parse(&shard.index_file).unwrap().unwrap();
        assert_eq!(descriptor.local_nlist().unwrap(), 2);
    }
    assert!(initial.iter().all(|old| !indexed.contains(old)));

    let mut search = table.new_batch_vector_search_builder();
    search
        .with_vector_column("embedding")
        .with_query_vectors(vec![vec![1., 0.], vec![103., 0.]])
        .with_limit(1)
        .with_options(HashMap::from([("centroid.nprobe".into(), "1".into())]));
    let results = search.execute().await.unwrap();
    assert_eq!(results[0].row_ids().unwrap().row_ids, [1]);
    assert_eq!(results[1].row_ids().unwrap().row_ids, [7]);
    assert_eq!(results[1].row_ids().unwrap().scores, [1.0]);

    // A second build atomically replaces the preceding centroid generation.
    assert_eq!(builder.execute().await.unwrap(), 3);
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
    for (start, pt) in [(0, 1), (2, 0), (4, 1)] {
        let vectors = build_vector_batch(vec![start, start + 1], vec![vec![1., 0.]; 2]);
        let mut fields = vectors.schema().fields().to_vec();
        fields.push(Arc::new(ArrowField::new("pt", ArrowDataType::Int32, true)));
        let mut columns = vectors.columns().to_vec();
        columns.push(Arc::new(Int32Array::from(vec![pt; 2])));
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
        let mut writer = TableWrite::new(&table, "centroid-partitions".into()).unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        TableCommit::new(table.clone(), "centroid-partitions".into())
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }

    let mut builder = table.new_global_index_build_builder();
    builder
        .with_index_column("embedding")
        .with_index_type("ivf-flat");
    assert_eq!(builder.execute().await.unwrap(), 3);
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
    let ranges: Vec<_> = plan
        .shards()
        .iter()
        .map(|shard| shard.row_range())
        .collect();
    assert_eq!(ranges, [RowRange::new(0, 1), RowRange::new(4, 5)]);
    let replacements = plan.replacement_messages();
    assert_eq!(replacements.len(), 1);
    assert_eq!(replacements[0].deleted_index_files.len(), 2);
    assert_ne!(replacements[0].partition, untouched.partition);
    assert_eq!(builder.execute().await.unwrap(), 4);
    let mut indexed = active_entries(&table).await;
    assert!(indexed.contains(untouched));
    let groups = centroid_index::group(&indexed).unwrap();
    assert_eq!(groups.len(), 2);
    assert!(groups
        .iter()
        .all(|group| group.descriptor.role == Role::Router(2)
            && group.shards.len() == 1
            && group.vector_count == 2));

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
async fn centroid_empty_router_does_not_define_raw_scoring() {
    use crate::table::SearchScanSplit;

    let path = "memory:/centroid_empty_metric";
    let mut options = vindex_e2e_options("8");
    options.insert("vector-index.search-mode".into(), "detail".into());
    let table = test_table_with_io(
        FileIOBuilder::new("memory").build().unwrap(),
        path,
        vindex_schema_builder(options).build().unwrap(),
    );
    setup_dirs(table.file_io(), path).await;
    let vectors = build_vector_batch(vec![0], vec![vec![0., 0.]]);
    let null_embedding = arrow_array::new_null_array(vectors.column(1).data_type(), 1);
    let nulls = RecordBatch::try_new(
        vectors.schema(),
        vec![vectors.column(0).clone(), null_embedding],
    )
    .unwrap();
    let mut writer = TableWrite::new(&table, "centroid-empty".into()).unwrap();
    writer.write_arrow_batch(&nulls).await.unwrap();
    TableCommit::new(table.clone(), "centroid-empty".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("diskann")
        .with_options(HashMap::from([
            ("diskann.sharding".into(), "centroid".into()),
            ("diskann.dimension".into(), "2".into()),
            ("diskann.metric".into(), "cosine".into()),
        ]))
        .execute()
        .await
        .unwrap();
    let entries = active_entries(&table).await;
    let groups = centroid_index::group(&entries).unwrap();
    let [empty] = groups.as_slice() else {
        panic!("expected one empty centroid group");
    };
    assert_eq!(empty.descriptor.role, Role::Router(0));
    assert_eq!(empty.router.index_file.file_size, 0);
    assert!(table
        .file_io()
        .exists(&format!(
            "{path}/index/{}",
            empty.router.index_file.file_name
        ))
        .await
        .unwrap());
    write_vectors(&table, vec![1, 2], vec![vec![1., 0.], vec![100., 0.]]).await;

    let mut search = table.new_vector_search_builder();
    search
        .with_vector_column("embedding")
        .with_query_vector(vec![1., 0.])
        .with_limit(1)
        .with_options(HashMap::from([("metric".into(), "l2".into())]));
    let plan = search.new_scan().unwrap().plan().await.unwrap();
    let raw = plan.splits().iter().find_map(|split| match split {
        SearchScanSplit::Raw {
            index_type,
            row_ranges,
            ..
        } => Some((index_type, row_ranges)),
        _ => None,
    });
    let (index_type, ranges) = raw.expect("the appended rows require raw scoring");
    assert_eq!(index_type, &None);
    assert_eq!(ranges.as_slice(), [RowRange::new(1, 2)]);
    let result = search.new_read().unwrap().read(plan).await.unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, [1]);
}

#[tokio::test]
async fn centroid_search_uses_live_rows_before_capping_candidates() {
    let path = "memory:/centroid_rerank_deleted";
    let mut options = vindex_e2e_options("8");
    options.insert("deletion-vectors.enabled".into(), "true".into());
    let table = test_table_with_io(
        FileIOBuilder::new("memory").build().unwrap(),
        path,
        vindex_schema_builder(options).build().unwrap(),
    );
    setup_dirs(table.file_io(), path).await;
    write_vectors(
        &table,
        vec![0, 1, 2],
        vec![vec![0., 0.], vec![1., 0.], vec![2., 0.]],
    )
    .await;
    let mut options = centroid_options();
    options.insert("ivf-flat.centroid.nlist".into(), "1".into());
    table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options)
        .execute()
        .await
        .unwrap();
    let writer = table.new_write_builder();
    let update = writer.new_update().unwrap();
    writer
        .new_commit()
        .commit(update.delete_by_row_id(vec![0]).await.unwrap())
        .await
        .unwrap();

    let mut search = table.new_vector_search_builder();
    search
        .with_vector_column("embedding")
        .with_query_vector(vec![0., 0.])
        .with_limit(1)
        .with_options(HashMap::from([("refine_factor".into(), "1".into())]));
    let plan = search.new_scan().unwrap().plan().await.unwrap();
    let reader = search.new_read().unwrap();
    let local = reader.read(plan.clone()).await.unwrap();
    let scheduled = reader
        .read_with_executor(plan, &Worker(&table))
        .await
        .unwrap();
    for result in [local, scheduled] {
        assert_eq!(result.row_ids().unwrap().row_ids, [1]);
    }

    search
        .with_filter(
            PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(2))
                .unwrap(),
        )
        .with_options(HashMap::from([(
            "vector-index.post-filter.candidates-factor".into(),
            "2".into(),
        )]));
    let plan = search.new_scan().unwrap().plan().await.unwrap();
    let reader = search.new_read().unwrap();
    let local = reader.read(plan.clone()).await.unwrap();
    let scheduled = reader
        .read_with_executor(plan, &Worker(&table))
        .await
        .unwrap();
    for result in [local, scheduled] {
        assert_eq!(result.row_ids().unwrap().row_ids, [2]);
    }
}

#[tokio::test]
async fn centroid_query_reads_java_model_and_native_shards_with_rebased_labels() {
    use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
    use paimon_vindex_core::io::PosWriter;

    let path = "memory:/centroid_java_fixture";
    let table = vindex_e2e_table(path, "8");
    setup_dirs(table.file_io(), path).await;
    let fixture = java_fixture::unpack();
    let source_meta = fs::read(fixture.path().join("source.meta")).unwrap();
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
    for (stem, row_count) in [("router", 0), ("shard-0", 2), ("shard-1", 2)] {
        let name = format!("{stem}.index");
        let bytes = fs::read(fixture.path().join(&name)).unwrap();
        let file_size = bytes.len() as i64;
        let meta = fs::read(fixture.path().join(format!("{stem}.meta"))).unwrap();
        table
            .file_io()
            .new_output(&format!("{path}/index/{name}"))
            .unwrap()
            .write(bytes.into())
            .await
            .unwrap();
        message.new_index_files.push(IndexFileMeta {
            index_type: "ivf-flat".into(),
            file_name: name,
            file_size,
            row_count,
            external_path: None,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 2,
                row_range_end: 5,
                index_field_id: find_index_field(&table, "embedding").unwrap().id(),
                extra_field_ids: None,
                index_meta: Some(meta),
                source_meta: Some(source_meta.clone()),
            }),
        });
    }
    let mut without_router = message.clone();
    without_router.new_index_files.remove(0);
    let error = TableCommit::new(table.clone(), "centroid-without-router".into())
        .commit(vec![without_router])
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Centroid generation has no routing model"));
    TableCommit::new(table.clone(), "java-centroid-generation".into())
        .commit(vec![message])
        .await
        .unwrap();
    let indexed = active_entries(&table).await;
    let groups = centroid_index::group(&indexed).unwrap();
    let group = &groups[0];
    assert_eq!(group.descriptor.role, Role::Router(2));
    assert_eq!(group.descriptor.router_metric().unwrap(), MetricType::L2);
    assert_eq!(group.local_nlists, [(0, 1), (1, 1)].into());
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

    let mut search = table.new_vector_search_builder();
    search
        .with_vector_column("embedding")
        .with_query_vector(vec![100., 0.])
        .with_limit(1)
        .with_filter(
            PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(3))
                .unwrap(),
        )
        .with_options(HashMap::from([
            ("centroid.nprobe".into(), "2".into()),
            (
                "vector-index.post-filter.candidates-factor".into(),
                "1".into(),
            ),
        ]));
    let plan = search.new_scan().unwrap().plan().await.unwrap();
    let reader = search.new_read().unwrap();
    let local = reader.read(plan.clone()).await.unwrap();
    let scheduled = reader
        .read_with_executor(plan, &Worker(&table))
        .await
        .unwrap();
    for result in [local, scheduled] {
        assert!(result.row_ids().unwrap().is_empty());
    }

    let batch_table = table.clone();
    std::thread::spawn(move || {
        futures::executor::block_on(async {
            let mut search = batch_table.new_batch_vector_search_builder();
            search
                .with_vector_column("embedding")
                .with_query_vectors(vec![vec![0., 0.], vec![100., 0.]])
                .with_limit(1)
                .with_options(HashMap::from([("centroid.nprobe".into(), "1".into())]));
            let plan = search.new_scan().unwrap().plan().await.unwrap();
            let reader = search.new_read().unwrap();
            let local = reader.read(plan.clone()).await.unwrap();
            let scheduled = reader
                .read_with_executor(plan, &Worker(&batch_table))
                .await
                .unwrap();
            for results in [local, scheduled] {
                assert_eq!(results[0].row_ids().unwrap().row_ids, [2]);
                assert_eq!(results[1].row_ids().unwrap().row_ids, [4]);
            }
        });
    })
    .join()
    .unwrap();

    // A native label of 4 would escape the routed range [2, 5] after rebasing.
    let config = VectorIndexConfig::from_options(&HashMap::from([
        ("index.type".into(), "ivf_flat".into()),
        ("dimension".into(), "2".into()),
        ("metric".into(), "l2".into()),
        ("nlist".into(), "1".into()),
    ]))
    .unwrap();
    let vectors = [100., 0., 101., 0.];
    let mut writer =
        VectorIndexWriter::new(VectorIndexTrainer::train(config, &vectors, 2).unwrap());
    writer.add_vectors(&[4, 3], &vectors, 2).unwrap();
    let mut bytes = Vec::new();
    writer.write(&mut PosWriter::new(&mut bytes)).unwrap();
    assert_eq!(bytes.len(), group.shards[&1].index_file.file_size as usize);
    table
        .file_io()
        .new_output(&format!("{path}/index/shard-1.index"))
        .unwrap()
        .write(bytes.into())
        .await
        .unwrap();
    let error = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![100., 0.])
        .with_limit(1)
        .with_options(HashMap::from([("centroid.nprobe".into(), "1".into())]))
        .execute()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("outside its centroid shard"));
}

#[tokio::test]
async fn centroid_bitmaps_prefilter_or_before_ranking_and_drop_with_the_vector_column() {
    use crate::spec::{Datum, Predicate, PredicateBuilder};
    let path = "memory:/centroid_bitmap_prefilter";
    let schema = vindex_schema_builder(vindex_e2e_options("8"))
        .column("status", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    let io = FileIOBuilder::new("memory").build().unwrap();
    let table = test_table_with_io(io, path, schema);
    setup_dirs(table.file_io(), path).await;
    let mut batch = build_vector_batch(
        (0..5).collect(),
        vec![
            vec![0., 0.],
            vec![0.1, 0.],
            vec![2., 0.],
            vec![100., 0.],
            vec![101., 0.],
        ],
    );
    let (schema, mut columns, rows) = batch.into_parts();
    columns.push(Arc::new(Int32Array::from(vec![
        Some(0),
        None,
        Some(9),
        Some(0),
        Some(0),
    ])));
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(ArrowField::new(
        "status",
        ArrowDataType::Int32,
        true,
    )));
    batch = RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap();
    let mut write = TableWrite::new(&table, "centroid-bitmaps-data".into()).unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    TableCommit::new(table.clone(), "centroid-bitmaps-data".into())
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let mut options = centroid_options();
    options.insert(
        "extra_fields".into(),
        r#"{"id":"bitmap","status":"bitmap"}"#.into(),
    );
    let mut build = table.new_global_index_build_builder();
    build
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options.clone());
    assert_eq!(build.execute().await.unwrap(), 5);
    let entries = active_entries(&table).await;
    let groups = centroid_index::group(&entries).unwrap();
    let group = &groups[0];
    assert_eq!(group.bitmaps.keys().copied().collect::<Vec<_>>(), [0, 2]);
    assert!(group
        .bitmaps
        .values()
        .all(|entry| entry.index_file.row_count == 5));
    let router_id = group
        .router
        .index_file
        .file_name
        .strip_prefix("vector-ivf-flat-global-index-training-model-")
        .and_then(|name| name.strip_suffix(".index"))
        .unwrap();
    assert!(uuid::Uuid::parse_str(router_id).is_ok());
    for entry in group.bitmaps.values() {
        let id = entry
            .index_file
            .file_name
            .strip_prefix("bitmap-global-index-")
            .and_then(|name| name.strip_suffix(".index"))
            .unwrap();
        assert!(uuid::Uuid::parse_str(id).is_ok());
    }

    // A shard with no bitmap match must not open the ANN payload at all.
    let field = table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == "id")
        .unwrap();
    let all_ids = crate::table::centroid_bitmap::BitmapPredicate::Leaf(
        field.id(),
        field.data_type().clone(),
        crate::spec::PredicateOperator::IsNotNull,
        vec![],
    );
    let matches = all_ids.matching(&table, group).await.unwrap();
    let mut skipped = false;
    for &centroid in group.shards.keys() {
        let matched =
            crate::table::centroid_bitmap::rows_for_centroid(&matches, group.range_size, centroid);
        if matched.contains(3) {
            table
                .file_io()
                .delete_file(&format!(
                    "{path}/index/{}",
                    group.shards[&centroid].index_file.file_name
                ))
                .await
                .unwrap();
            skipped = true;
        }
    }
    assert!(skipped);

    let predicate = PredicateBuilder::new(table.schema().fields());
    let filter = Predicate::or(vec![
        predicate.equal("id", Datum::Int(2)).unwrap(),
        predicate.is_null("status").unwrap(),
    ]);
    let mut search = table.new_vector_search_builder();
    search
        .with_vector_column("embedding")
        .with_query_vector(vec![0., 0.])
        .with_limit(2)
        .with_filter(filter)
        .with_options(HashMap::from([
            ("centroid.nprobe".into(), "2".into()),
            (
                "vector-index.post-filter.candidates-factor".into(),
                "1".into(),
            ),
        ]));
    let plan = search.new_scan().unwrap().plan().await.unwrap();
    let read = search.new_read().unwrap();
    let result = read
        .read_with_executor(plan.clone(), &Worker(&table))
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, [1, 2]);
    let buffered = std::thread::scope(|scope| {
        scope
            .spawn(|| futures::executor::block_on(read.read(plan.clone())))
            .join()
            .unwrap()
    })
    .unwrap();
    assert_eq!(buffered.row_ids().unwrap().row_ids, [1, 2]);

    let mut sources = plan.splits().to_vec();
    sources.push(crate::table::SearchScanSplit::Raw {
        row_ranges: vec![RowRange::new(3, 3)],
        scalar_index_files: vec![],
        index_type: Some("ivf-flat".into()),
    });
    let explicit = crate::table::VectorScanPlan::new(plan.snapshot().cloned(), sources).unwrap();
    let explicit: crate::table::VectorScanPlan =
        serde_json::from_value(serde_json::to_value(explicit).unwrap()).unwrap();
    let read = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![0., 0.])
        .with_limit(1)
        .with_filter(predicate.equal("id", Datum::Int(3)).unwrap())
        .with_options(HashMap::from([("centroid.nprobe".into(), "1".into())]))
        .new_read()
        .unwrap();
    for result in [
        read.read(explicit.clone()).await.unwrap(),
        read.read_with_executor(explicit, &Worker(&table))
            .await
            .unwrap(),
    ] {
        assert_eq!(result.row_ids().unwrap().row_ids, [3]);
    }

    options.insert("extra_fields".into(), r#"{"status":"bitmap"}"#.into());
    build.with_options(options);
    assert_eq!(build.execute().await.unwrap(), 4);
    let rebuilt = active_entries(&table).await;
    let rebuilt_groups = centroid_index::group(&rebuilt).unwrap();
    assert_eq!(rebuilt_groups[0].bitmaps.len(), 1);
    assert_eq!(
        rebuilt_groups[0]
            .router
            .index_file
            .global_index_meta
            .as_ref()
            .unwrap()
            .extra_field_ids
            .as_ref()
            .unwrap()
            .len(),
        1
    );
    assert!(rebuilt.iter().all(|entry| !entries.contains(entry)));
    let mut drop_index = table.new_global_index_drop_builder();
    drop_index
        .with_index_column("embedding")
        .with_index_type("ivf-flat");
    assert_eq!(drop_index.execute().await.unwrap(), 4);
}

#[tokio::test]
async fn centroid_bitmaps_compare_numeric_literals_by_value() {
    use crate::spec::{DecimalType, FloatType, TimestampType};
    use arrow_array::{Decimal128Array, Float32Array, TimestampMillisecondArray};

    let path = "memory:/centroid_bitmap_numeric_keys";
    let schema = vindex_schema_builder(vindex_e2e_options("8"))
        .column("ratio", DataType::Float(FloatType::new()))
        .column("amount", DataType::Decimal(DecimalType::new(4, 1).unwrap()))
        .column(
            "event_time",
            DataType::Timestamp(TimestampType::new(3).unwrap()),
        )
        .build()
        .unwrap();
    let table = test_table_with_io(FileIOBuilder::new("memory").build().unwrap(), path, schema);
    setup_dirs(table.file_io(), path).await;
    let vectors = build_vector_batch(vec![0, 1], vec![vec![0., 0.], vec![1., 0.]]);
    let mut fields = vectors.schema().fields().to_vec();
    let mut columns = vectors.columns().to_vec();
    for (name, column) in [
        (
            "ratio",
            Arc::new(Float32Array::from(vec![-0.0, 0.0])) as ArrayRef,
        ),
        (
            "amount",
            Arc::new(
                Decimal128Array::from(vec![100, 10])
                    .with_precision_and_scale(4, 1)
                    .unwrap(),
            ),
        ),
        (
            "event_time",
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
        ),
    ] {
        fields.push(Arc::new(ArrowField::new(
            name,
            column.data_type().clone(),
            true,
        )));
        columns.push(column);
    }
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
    let mut write = TableWrite::new(&table, "centroid-numeric-data".into()).unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    TableCommit::new(table.clone(), "centroid-numeric-data".into())
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let mut options = centroid_options();
    options.insert("ivf-flat.centroid.nlist".into(), "1".into());
    options.insert(
        "extra_fields".into(),
        r#"{"ratio":"bitmap","amount":"bitmap","event_time":"bitmap"}"#.into(),
    );
    table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options)
        .execute()
        .await
        .unwrap();

    let predicate = PredicateBuilder::new(table.schema().fields());
    let (index, ratio) = table
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == "ratio")
        .unwrap();
    for (predicate, expected) in [
        (
            predicate.equal("ratio", Datum::Float(0.0)).unwrap(),
            &[1][..],
        ),
        (
            crate::spec::Predicate::Leaf {
                column: "ratio".into(),
                index,
                data_type: ratio.data_type().clone(),
                op: crate::spec::PredicateOperator::Eq,
                literals: vec![Datum::Double(0.0)],
            },
            &[1][..],
        ),
        (
            predicate
                .equal(
                    "amount",
                    Datum::Decimal {
                        unscaled: 100,
                        precision: 4,
                        scale: 2,
                    },
                )
                .unwrap(),
            &[1][..],
        ),
        (
            predicate
                .equal(
                    "event_time",
                    Datum::Timestamp {
                        millis: 1000,
                        nanos: 500_000,
                    },
                )
                .unwrap(),
            &[][..],
        ),
    ] {
        let mut search = table.new_vector_search_builder();
        search
            .with_vector_column("embedding")
            .with_query_vector(vec![0., 0.])
            .with_limit(1)
            .with_filter(predicate.clone());
        let plan = search.new_scan().unwrap().plan().await.unwrap();
        let result = search
            .new_read()
            .unwrap()
            .read_with_executor(plan, &Worker(&table))
            .await
            .unwrap();
        assert_eq!(result.row_ids().unwrap().row_ids, expected, "{predicate:?}");
    }
}

#[tokio::test]
async fn centroid_bitmaps_allow_reusing_a_plan_with_a_different_filter() {
    let path = "memory:/centroid_bitmap_reused_plan";
    let mut options = vindex_e2e_options("8");
    options.insert("vector-index.search-mode".into(), "fast".into());
    options.insert("scalar-index.prefilter.search-mode".into(), "full".into());
    let schema = vindex_schema_builder(options)
        .column("pt", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .build()
        .unwrap();
    let table = test_table_with_io(FileIOBuilder::new("memory").build().unwrap(), path, schema);
    setup_dirs(table.file_io(), path).await;
    for (pt, id, coordinate) in [(0, 10, 0.0), (1, 20, 100.0)] {
        let vectors = build_vector_batch(
            vec![id, id + 1],
            vec![vec![coordinate, 0.], vec![coordinate + 1., 0.]],
        );
        let mut fields = vectors.schema().fields().to_vec();
        fields.push(Arc::new(ArrowField::new("pt", ArrowDataType::Int32, true)));
        let mut columns = vectors.columns().to_vec();
        columns.push(Arc::new(Int32Array::from(vec![pt; 2])));
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
        let mut write = TableWrite::new(&table, "centroid-bitmap-partitions".into()).unwrap();
        write.write_arrow_batch(&batch).await.unwrap();
        TableCommit::new(table.clone(), "centroid-bitmap-partitions".into())
            .commit(write.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }
    let predicate = PredicateBuilder::new(table.schema().fields());
    let mut options = centroid_options();
    options.insert("extra_fields".into(), r#"{"id":"bitmap"}"#.into());
    for (column, kind, pt, options) in [
        ("embedding", "ivf-flat", 0, options),
        (
            "embedding",
            "ivf-flat",
            1,
            HashMap::from([("ivf-flat.metric".into(), "l2".into())]),
        ),
        ("id", "btree", 1, HashMap::new()),
    ] {
        table
            .new_global_index_build_builder()
            .with_index_column(column)
            .with_index_type(kind)
            .with_options(options)
            .with_partition_filter(predicate.equal("pt", Datum::Int(pt)).unwrap())
            .unwrap()
            .execute()
            .await
            .unwrap();
    }

    let plan = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_filter(predicate.equal("id", Datum::Int(10)).unwrap())
        .new_scan()
        .unwrap()
        .plan()
        .await
        .unwrap();
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![0., 0.])
        .with_limit(1)
        .with_filter(predicate.greater_than("id", Datum::Int(9)).unwrap())
        .new_read()
        .unwrap()
        .read(plan)
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, [0]);
}

#[tokio::test]
async fn centroid_bitmaps_check_referenced_fields_after_schema_changes() {
    use crate::spec::{BigIntType, SchemaChange};

    let path = "memory:/centroid_bitmap_schema_id";
    let table = vindex_e2e_table(path, "4");
    setup_dirs(table.file_io(), path).await;
    write_vectors(&table, vec![10, 11], vec![vec![0., 0.], vec![1., 0.]]).await;
    let mut options = centroid_options();
    options.insert("ivf-flat.centroid.nlist".into(), "1".into());
    options.insert("extra_fields".into(), r#"{"id":"bitmap"}"#.into());
    table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options)
        .execute()
        .await
        .unwrap();

    let entries = active_entries(&table).await;
    let groups = centroid_index::group(&entries).unwrap();
    let group = &groups[0];
    assert_eq!(group.bitmap_schema_id, Some(table.schema().id()));
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(table.schema().id()))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    let compatible_schema = table
        .schema()
        .apply_changes(vec![SchemaChange::rename_column("id".into(), "key".into())])
        .unwrap();
    let compatible = table
        .copy_with_resolved_schema(compatible_schema, table.branch())
        .unwrap();
    let filter = PredicateBuilder::new(compatible.schema().fields())
        .equal("key", Datum::Int(11))
        .unwrap();
    let result = compatible
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![0., 0.])
        .with_limit(1)
        .with_options(HashMap::from([(
            "vector-index.post-filter.candidates-factor".into(),
            "1".into(),
        )]))
        .with_filter(filter)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, [1]);

    let changed_schema = compatible
        .schema()
        .apply_changes(vec![SchemaChange::update_column_type(
            "key".into(),
            DataType::BigInt(BigIntType::new()),
        )])
        .unwrap();
    let changed = table
        .copy_with_resolved_schema(changed_schema, table.branch())
        .unwrap();
    // The type change uses row filtering without opening the old Bitmap files.
    for entry in group.bitmaps.values() {
        table
            .file_io()
            .delete_file(&format!("{path}/index/{}", entry.index_file.file_name))
            .await
            .unwrap();
    }
    let filter = PredicateBuilder::new(changed.schema().fields())
        .equal("key", Datum::Long(11))
        .unwrap();
    let result = changed
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![0., 0.])
        .with_limit(1)
        .with_filter(filter)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, [1]);
}

#[tokio::test]
async fn centroid_bitmap_update_under_ignore_requires_dropping_the_generation() {
    let path = "memory:/centroid_bitmap_update";
    let table = vindex_e2e_table(path, "8");
    setup_dirs(table.file_io(), path).await;
    write_vectors(&table, vec![7, 8], vec![vec![0., 0.], vec![10., 0.]]).await;
    let mut options = centroid_options();
    options.insert("extra_fields".into(), r#"{"id":"bitmap"}"#.into());
    let ignoring = table.copy_with_options(HashMap::from([(
        "global-index.column-update-action".into(),
        "IGNORE".into(),
    )]));
    let error = ignoring
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options.clone())
        .execute()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Centroid Bitmap fields require"));

    table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options)
        .execute()
        .await
        .unwrap();
    table
        .new_global_index_build_builder()
        .with_index_column("id")
        .with_index_type("btree")
        .execute()
        .await
        .unwrap();
    let before = active_entries(&table).await;
    assert_eq!(centroid_index::group(&before).unwrap().len(), 1);

    let update = RecordBatch::try_from_iter([
        (
            crate::spec::ROW_ID_FIELD_NAME,
            Arc::new(arrow_array::Int64Array::from(vec![0])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(vec![42])) as ArrayRef),
    ])
    .unwrap();
    let commit_user = "centroid-bitmap-update";
    let mut messages = crate::table::TableUpdate::new(&ignoring, commit_user.into())
        .update_by_arrow_with_row_id(vec![update])
        .await
        .unwrap();
    let commit = TableCommit::new(ignoring.clone(), commit_user.into());
    let error = commit.commit(messages.clone()).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("active centroid index with Bitmap"));
    assert_eq!(active_entries(&table).await, before);

    let router = centroid_index::group(&before).unwrap()[0]
        .router
        .index_file
        .clone();
    let mut partial_drop = messages.clone();
    partial_drop[0].deleted_index_files.push(router);
    let error = commit.commit(partial_drop).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("active centroid index with Bitmap"));
    assert_eq!(active_entries(&table).await, before);

    let mut drop_index = ignoring.new_global_index_drop_builder();
    drop_index
        .with_index_column("embedding")
        .with_index_type("ivf-flat");
    messages.extend(drop_index.build().await.unwrap());
    commit.commit(messages).await.unwrap();
    let remaining = active_entries(&table).await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].index_file.index_type, "btree");
}

#[tokio::test]
async fn centroid_bitmap_spill_preserves_sparse_source_labels() {
    use crate::spec::PredicateOperator;
    use crate::table::bitmap_global_index_reader::BitmapGlobalIndexReader;

    let path = "memory:/centroid_bitmap_spill";
    let table = vindex_e2e_table(path, "8");
    setup_dirs(table.file_io(), path).await;
    for (batch_no, (ids, present)) in [
        ([7, 90, 8], [true, false, true]),
        ([93, 94, 95], [false; 3]),
        ([91, 7, 92], [false, true, false]),
    ]
    .into_iter()
    .enumerate()
    {
        let template = build_vector_batch(ids.to_vec(), vec![vec![0., 0.]; 3]);
        let mut vectors = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        for (row, present) in present.into_iter().enumerate() {
            if present {
                vectors.values().append_value(row as f32);
                vectors.values().append_value(batch_no as f32);
            }
            vectors.append(present);
        }
        let batch = RecordBatch::try_new(
            template.schema(),
            vec![template.column(0).clone(), Arc::new(vectors.finish())],
        )
        .unwrap();
        let commit_user = format!("centroid-bitmap-spill-{batch_no}");
        let mut write = TableWrite::new(&table, commit_user.clone()).unwrap();
        write.write_arrow_batch(&batch).await.unwrap();
        TableCommit::new(table.clone(), commit_user)
            .commit(write.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }

    let mut options = centroid_options();
    options.insert("ivf-flat.centroid.nlist".into(), "1".into());
    options.insert("extra_fields".into(), r#"{"id":"bitmap"}"#.into());
    options.insert("write-buffer-size".into(), "80b".into());
    options.insert("bitmap-index.dictionary-block-size".into(), "9b".into());
    table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(options)
        .execute()
        .await
        .unwrap();
    let entries = active_entries(&table).await;
    let groups = centroid_index::group(&entries).unwrap();
    assert_eq!(groups.len(), 1);
    let bitmap = &groups[0].bitmaps[&0].index_file;
    assert_eq!(bitmap.row_count, 3);
    let input = table
        .file_io()
        .new_input(&format!("{path}/index/{}", bitmap.file_name))
        .unwrap()
        .reader()
        .await
        .unwrap();
    let reader = BitmapGlobalIndexReader::open(Box::new(input), bitmap.file_size as u64)
        .await
        .unwrap();
    let field = &table.schema().fields()[0];
    for (op, literals, expected) in [
        (PredicateOperator::Eq, vec![Datum::Int(7)], vec![0, 7]),
        (PredicateOperator::Eq, vec![Datum::Int(8)], vec![2]),
        (PredicateOperator::IsNotNull, vec![], vec![0, 2, 7]),
    ] {
        let matches = reader
            .query(op, &literals, field.data_type())
            .await
            .unwrap();
        assert_eq!(matches.into_iter().collect::<Vec<_>>(), expected);
    }
}
