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
use crate::table::vindex_index_build_builder::extraction::{extract_vector_batch, local_ids};
use arrow_array::Array;
use arrow_buffer::NullBuffer;
use paimon_vindex_core::index::VectorIndexReader;
use std::io::Cursor;

fn nullable_batch(rows: Vec<Option<Vec<Option<f32>>>>, start: i64) -> RecordBatch {
    let mut vectors = ListBuilder::new(Float32Builder::new());
    for row in &rows {
        if let Some(row) = row {
            for value in row {
                vectors.values().append_option(*value);
            }
            vectors.append(true);
        } else {
            vectors.append(false);
        }
    }
    let vectors = vectors.finish();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("embedding", vectors.data_type().clone(), true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(vectors),
            Arc::new(Int64Array::from(
                (start..start + rows.len() as i64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[test]
fn null_rows_are_compacted_without_renumbering_or_accepting_null_ids() {
    let batch = nullable_batch(
        vec![
            None,
            Some(vec![Some(1.), Some(2.)]),
            None,
            Some(vec![Some(3.), Some(4.)]),
            None,
        ],
        41,
    )
    .slice(1, 3);
    let mut expected = 42;
    let result = validate_vector_batch(&batch, "embedding", 2, &mut expected).unwrap();
    assert_eq!(expected, 45);
    assert_eq!(result.source_rows, 3);
    assert_eq!(result.vector_count, 2);
    assert_eq!(result.values.as_ref(), &[1., 2., 3., 4.]);
    assert_eq!(result.row_ids.as_ref(), &[42, 44]);
    assert_eq!(local_ids(&result.row_ids, 40, 10).unwrap(), vec![2, 4]);
    assert_eq!(result.bytes().len(), 16);
    let bad = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            batch.schema().field(0).clone(),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ])),
        vec![
            batch.column(0).clone(),
            Arc::new(Int64Array::from(vec![Some(42), None, Some(44)])),
        ],
    )
    .unwrap();
    assert!(extract_vector_batch(&bad, "embedding", 2).is_err());
}

#[test]
fn fixed_size_null_rows_ignore_hidden_child_nulls_and_preserve_slices() {
    let array = arrow_array::FixedSizeListArray::new(
        Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
        2,
        Arc::new(arrow_array::Float32Array::from(vec![
            None,
            None,
            Some(1.),
            Some(2.),
            None,
            None,
            Some(3.),
            Some(4.),
            None,
            None,
        ])),
        Some(NullBuffer::from(vec![false, true, false, true, false])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("embedding", array.data_type().clone(), true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(array),
            Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14])),
        ],
    )
    .unwrap()
    .slice(1, 3);
    let vectors = extract_vector_batch(&batch, "embedding", 2).unwrap();
    assert_eq!(vectors.values.as_ref(), &[1., 2., 3., 4.]);
    assert_eq!(vectors.row_ids.as_ref(), &[11, 13]);
    assert_eq!(vectors.source_rows, 3);
    assert!(extract_vector_batch(&batch, "embedding", 3).is_err());
}

#[test]
fn null_rows_do_not_hide_invalid_dimensions_elements_or_row_ranges() {
    for vector in [vec![Some(1.)], vec![Some(1.), None]] {
        let batch = nullable_batch(vec![None, Some(vector), None], 100);
        assert!(extract_vector_batch(&batch, "embedding", 2).is_err());
    }
    let batch = nullable_batch(vec![None, Some(vec![Some(1.), Some(2.)])], 100);
    let mut expected = 99;
    assert!(validate_vector_batch(&batch, "embedding", 2, &mut expected).is_err());
    let mut range_index = 0;
    let mut expected = 100;
    assert!(validate_vector_batch_ranges(
        &batch,
        "embedding",
        2,
        &[RowRange::new(100, 100)],
        &mut range_index,
        &mut expected
    )
    .is_err());
}

#[test]
fn dense_batches_keep_borrowed_arrow_storage() {
    let batch = nullable_batch(
        vec![
            Some(vec![Some(1.), Some(2.)]),
            Some(vec![Some(3.), Some(4.)]),
        ],
        0,
    );
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<arrow_array::Float32Array>()
        .unwrap();
    let vectors = extract_vector_batch(&batch, "embedding", 2).unwrap();
    assert_eq!(vectors.values.as_ptr(), values.values().as_ptr());
}

async fn append_nullable(table: &Table, rows: Vec<Option<Vec<Option<f32>>>>) {
    let batch = nullable_batch(rows, 0);
    let ids: ArrayRef = Arc::new(Int32Array::from(
        (0..batch.num_rows() as i32).collect::<Vec<_>>(),
    ));
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("embedding", batch.column(0).data_type().clone(), true),
        ])),
        vec![ids, batch.column(0).clone()],
    )
    .unwrap();
    let mut writer = TableWrite::new(table, "sparse".into()).unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    TableCommit::new(table.clone(), "sparse".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn metadata(
    table: &Table,
    file: &IndexFileMeta,
) -> paimon_vindex_core::index::VectorIndexMetadata {
    let path = file
        .external_path
        .clone()
        .unwrap_or_else(|| format!("{}/index/{}", table.location(), file.file_name));
    let input = table.file_io().new_input(&path).unwrap();
    VectorIndexReader::open(Cursor::new(input.read().await.unwrap()))
        .unwrap()
        .metadata()
}

#[tokio::test]
async fn sparse_and_all_null_shards_obey_prepare_and_cardinality_contract() {
    for kind in ["ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann"] {
        for granule in [false, true] {
            let table = vindex_e2e_table(&format!("memory:/sparse_{kind}_{granule}"), "10");
            setup_dirs(table.file_io(), table.location()).await;
            append_nullable(
                &table,
                vec![
                    None,
                    Some(vec![Some(1.), Some(0.)]),
                    None,
                    Some(vec![Some(0.), Some(1.)]),
                    None,
                ],
            )
            .await;
            let mut builder = table.new_global_index_build_builder();
            let mut options = HashMap::from([(format!("{kind}.dimension"), "2".into())]);
            if kind != "diskann" {
                options.insert(format!("{kind}.nlist"), "1".into());
                options.insert("vindex.build.granule.enabled".into(), granule.to_string());
            }
            builder
                .with_index_column("embedding")
                .with_index_type(kind)
                .with_options(options);
            let messages = builder.build().await.unwrap();
            assert_eq!(messages.len(), 1, "{kind}, granule={granule}");
            let file = &messages[0].new_index_files[0];
            assert_eq!(file.row_count, 5);
            assert_eq!(file.global_index_meta.as_ref().unwrap().row_range_start, 0);
            assert_eq!(metadata(&table, file).await.total_vectors, 2);
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                1
            );
            TableCommit::new(table.clone(), "sparse-index".into())
                .commit(messages)
                .await
                .unwrap();
            let result = table
                .new_vector_search_builder()
                .with_vector_column("embedding")
                .with_query_vector(vec![1., 1.])
                .with_limit(10)
                .execute()
                .await
                .unwrap();
            let mut ids = result.row_ids().unwrap().row_ids.clone();
            ids.sort_unstable();
            assert_eq!(ids, vec![1, 3], "{kind}, granule={granule}");
            assert!(builder.build().await.unwrap().is_empty());
            append_nullable(&table, vec![None, None, None]).await;
            let before = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id();
            assert!(builder.build().await.unwrap().is_empty());
            assert_eq!(builder.execute().await.unwrap(), 0);
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                before
            );
        }
    }
}

#[tokio::test]
async fn automatic_ivf_sizing_uses_valid_vectors_and_declared_row_count_includes_nulls() {
    let table = vindex_e2e_table("memory:/sparse_auto", "10000");
    setup_dirs(table.file_io(), table.location()).await;
    let mut rows = vec![None; 1000];
    rows[31] = Some(vec![Some(1.), Some(0.)]);
    rows[901] = Some(vec![Some(0.), Some(1.)]);
    append_nullable(&table, rows).await;
    let messages = table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-flat")
        .with_options(HashMap::from([
            ("ivf-flat.nlist".into(), "auto".into()),
            ("ivf-flat.train.sample-ratio".into(), "0.5".into()),
        ]))
        .build()
        .await
        .unwrap();
    let file = &messages[0].new_index_files[0];
    assert_eq!(file.row_count, 1000);
    let meta = metadata(&table, file).await;
    assert_eq!(meta.total_vectors, 2);
    assert_eq!(
        meta.nlist,
        paimon_vindex_core::autotune::infer_ivf_nlist(2).unwrap()
    );
}

#[tokio::test]
async fn null_in_rest_granules_discards_private_training_then_builds_sparse_index() {
    use super::super::pipeline::GranuleBuildOutcome;
    use super::super::VindexIndexBuildBuilder;
    use crate::vindex::VindexVectorIndexOptions;
    use parquet::arrow::AsyncArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    const FILES: usize = 1024;
    const ROWS: usize = 256;
    let table = vindex_e2e_table("memory:/sparse_rest", "1000000");
    setup_dirs(table.file_io(), table.location()).await;
    let dense = nullable_batch(vec![Some(vec![Some(1.), Some(0.)]); ROWS], 0);
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS))
        .set_offset_index_disabled(true)
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let serialize = |batch: RecordBatch| {
        let properties = properties.clone();
        async move {
            let mut bytes = Vec::new();
            let mut writer =
                AsyncArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
            writer.write(&batch).await.unwrap();
            writer.close().await.unwrap();
            bytes::Bytes::from(bytes)
        }
    };
    let dense_bytes = serialize(dense).await;
    let mut files = Vec::new();
    for i in 0..FILES {
        let name = format!("data-{i:04}.parquet");
        table
            .file_io()
            .new_output(&format!("{}/bucket-0/{name}", table.location()))
            .unwrap()
            .write(dense_bytes.clone())
            .await
            .unwrap();
        let mut file = data_file(&name, Some((i * ROWS) as i64), ROWS as i64);
        file.file_size = dense_bytes.len() as i64;
        file.file_source = Some(0);
        files.push(file);
    }
    let plan = |files: Vec<DataFileMeta>| {
        plan_vindex_shards(
            table.location(),
            table.schema().partition_keys(),
            table.schema().fields(),
            &CoreOptions::new(table.schema().options()),
            1,
            files
                .into_iter()
                .map(|file| {
                    ManifestEntry::new(
                        FileKind::Add,
                        BinaryRow::new(0).to_serialized_bytes(),
                        0,
                        1,
                        file,
                        2,
                    )
                })
                .collect(),
            1_000_000,
            &[],
        )
        .unwrap()
    };
    let builder = VindexIndexBuildBuilder::new(&table, IVF_FLAT_IDENTIFIER);
    let dense_plan = builder
        .plan_granules(
            &plan(files.clone())[0],
            "embedding",
            FILES * ROWS / 8,
            FILES * ROWS / 8,
        )
        .await
        .unwrap();
    assert!(!dense_plan.rest.is_empty());
    let null_row = dense_plan.rest.last().unwrap().to();
    let file = &mut files[null_row as usize / ROWS];
    let mut rows = vec![Some(vec![Some(1.), Some(0.)]); ROWS];
    rows[null_row as usize % ROWS] = None;
    let sparse_bytes = serialize(nullable_batch(rows, 0)).await;
    table
        .file_io()
        .new_output(&format!("{}/bucket-0/{}", table.location(), file.file_name))
        .unwrap()
        .write(sparse_bytes.clone())
        .await
        .unwrap();
    file.file_size = sparse_bytes.len() as i64;
    for file in &mut files {
        file.first_row_id = None;
    }
    TableCommit::new(table.clone(), "sparse-rest".into())
        .commit(vec![CommitMessage::new(
            BinaryRow::new(0).to_serialized_bytes(),
            0,
            files,
        )])
        .await
        .unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let entries = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan_manifest_entries(&snapshot)
        .await
        .unwrap();
    let shards = plan_vindex_shards(
        table.location(),
        table.schema().partition_keys(),
        table.schema().fields(),
        &CoreOptions::new(table.schema().options()),
        snapshot.id(),
        entries,
        1_000_000,
        &[],
    )
    .unwrap();
    let shard = &shards[0];
    let actual_plan = builder
        .plan_granules(shard, "embedding", FILES * ROWS / 8, FILES * ROWS / 8)
        .await
        .unwrap();
    assert_eq!(actual_plan.first, dense_plan.first);
    assert_eq!(actual_plan.rest, dense_plan.rest);
    assert!(actual_plan
        .first
        .iter()
        .all(|range| !(range.from()..=range.to()).contains(&null_row)));
    let user_options = HashMap::from([("ivf-flat.train.sample-ratio".into(), "0.125".into())]);
    let field = find_index_field(&table, "embedding").unwrap();
    let options = VindexVectorIndexOptions::new(
        table.schema().options(),
        &user_options,
        IVF_FLAT_IDENTIFIER,
        field,
    )
    .unwrap();
    let outcome = builder
        .build_index_file_granule(
            shard,
            "embedding",
            2,
            field.id(),
            &options,
            serde_json::to_vec(&options.native_options).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, GranuleBuildOutcome::Sparse));
    let root = format!("{}/index", table.location());
    assert!(table.file_io().list_status(&root).await.unwrap().is_empty());
    let messages = table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type(IVF_FLAT_IDENTIFIER)
        .with_options(user_options)
        .build()
        .await
        .unwrap();
    let file = &messages[0].new_index_files[0];
    assert_eq!(file.row_count, (FILES * ROWS) as i64);
    assert_eq!(
        metadata(&table, file).await.total_vectors,
        (FILES * ROWS - 1) as i64
    );
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id(),
        1
    );
}

#[tokio::test]
async fn automatic_pq_budget_is_validated_against_non_null_cardinality() {
    let table = vindex_e2e_table("memory:/sparse_budget", "10000");
    setup_dirs(table.file_io(), table.location()).await;
    append_nullable(
        &table,
        (0..2000)
            .map(|i| (i % 2 == 1).then_some(vec![Some(1.), Some(0.)]))
            .collect(),
    )
    .await;
    let messages = table
        .new_global_index_build_builder()
        .with_index_column("embedding")
        .with_index_type("ivf-pq")
        .with_options(HashMap::from([
            ("ivf-pq.dimension".into(), "2".into()),
            ("ivf-pq.max-bytes-per-vector".into(), "64".into()),
        ]))
        .build()
        .await
        .unwrap();
    let file = &messages[0].new_index_files[0];
    assert_eq!(file.row_count, 2000);
    assert_eq!(metadata(&table, file).await.total_vectors, 1000);
}
