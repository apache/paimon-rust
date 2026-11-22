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
use crate::io::{FileIO, FileIOBuilder};
use crate::spec::{BinaryRow, FileKind, GlobalIndexMeta, IndexFileMeta, VectorIndexOptions};
use crate::vector_search::GlobalIndexIOMeta;
use paimon_vindex_core::ivfflat::IVFFlatIndex;
use paimon_vindex_core::ivfflat_io::write_ivfflat_index;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

const ROOT: &str = "memory:/centroid-joint";
const ROWS: usize = 192;

fn flat_shard(nlist: usize, base: f32, first_id: usize, rows_per_list: usize) -> Vec<u8> {
    let centers = (0..nlist)
        .map(|i| base + 10.0 * i as f32)
        .collect::<Vec<_>>();
    let mut index = IVFFlatIndex::new(1, nlist, MetricType::L2);
    index.set_quantizer_centroids(centers.clone());
    let data = centers
        .into_iter()
        .flat_map(|center| std::iter::repeat_n(center, rows_per_list))
        .collect::<Vec<_>>();
    let labels = (first_id..first_id + data.len())
        .map(|label| label as i64)
        .collect::<Vec<_>>();
    index.add(&data, &labels, data.len());
    let mut bytes = Cursor::new(Vec::new());
    write_ivfflat_index(&index, &mut bytes).unwrap();
    bytes.into_inner()
}

#[test]
fn joint_probe_uses_sixteen_lists_and_retries_an_incomplete_shard() {
    let bytes = flat_shard(64, 0.0, 0, 1);
    let meta = GlobalIndexIOMeta::new("joint".into(), bytes.len() as u64, vec![]);
    let mut reader = VindexVectorGlobalIndexReader::new(meta, HashMap::new());
    let queries = [
        VectorSearch::new(vec![0.0], 16, "embedding".into())
            .unwrap()
            .with_options(HashMap::from([("ivf.nprobe".into(), "1".into())])),
        VectorSearch::new(vec![0.0], 17, "embedding".into())
            .unwrap()
            .with_options(HashMap::from([("ivf.nprobe".into(), "1".into())])),
        VectorSearch::new(vec![0.0], 20, "embedding".into())
            .unwrap()
            .with_options(HashMap::from([("ivf.nprobe".into(), "64".into())])),
    ];
    let probes = [Probe::CheckLayout, Probe::CheckLayout, Probe::Joint];
    let result = search_file(&mut reader, &queries, &probes, Cursor::new(bytes), |_| {
        Ok(())
    })
    .unwrap();
    assert_eq!(result[0].standard.as_ref().unwrap().len(), 1);
    let joint = result[0].joint.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(joint.len(), 16);
    assert!((0..16).all(|id| joint.contains_key(&id)));
    assert_eq!(result[1].joint.as_ref(), Some(&result[1].standard));
    assert_eq!(result[1].standard.as_ref().unwrap().len(), 1);
    let retried = result[2].joint.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(retried.len(), 20);
    assert!((0..20).all(|id| retried.contains_key(&id)));
}

fn entry(role: Role, size: usize, rows: usize, payload: Option<&[u8]>) -> IndexManifestEntry {
    let file_name = match role {
        Role::Router(_) => "router".into(),
        Role::Shard(id) => format!("shard-{id}"),
        _ => unreachable!(),
    };
    IndexManifestEntry {
        version: 1,
        kind: FileKind::Add,
        partition: BinaryRow::new(0).to_serialized_bytes(),
        bucket: 0,
        index_file: IndexFileMeta {
            index_type: "ivf-flat".into(),
            file_name,
            file_size: size as i64,
            row_count: rows as i64,
            external_path: None,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: (8 * ROWS - 1) as i64,
                index_field_id: 0,
                extra_field_ids: None,
                index_meta: Some(
                    Descriptor {
                        model_id: "test-router",
                        version: 1,
                        role,
                        payload,
                    }
                    .encode()
                    .unwrap(),
                ),
                source_meta: None,
            }),
        },
    }
}

async fn store(io: &FileIO, entry: &IndexManifestEntry, bytes: Vec<u8>) {
    io.new_output(&format!("{ROOT}/index/{}", entry.index_file.file_name))
        .unwrap()
        .write(bytes.into())
        .await
        .unwrap();
}

async fn run(
    io: &FileIO,
    entries: &[IndexManifestEntry],
    options: HashMap<String, String>,
) -> Vec<ScoredRowIds> {
    let queries = [0.0, 70.0]
        .into_iter()
        .map(|value| VectorSearch::new(vec![value], 10, "embedding".into()).unwrap())
        .collect();
    let options = VectorIndexOptions {
        index_type: Some("ivf-flat".into()),
        metric: "l2".into(),
        options,
    };
    let table_options = HashMap::new();
    execute_work(
        VectorSearchEvaluation {
            table: None,
            file_io: io,
            table_path: ROOT,
            table_options: &table_options,
            schema_fields: &[],
            next_row_id: None,
            filter: None,
        },
        VectorIndexWork {
            entry: entries[0].clone(),
            centroid_files: entries.to_vec(),
            queries,
            resolved: Some(options),
            filter: None,
            top_ks: vec![],
        },
        VectorMetricCheck {
            persisted: Arc::new(Mutex::new(None)),
            requested: None,
        },
        RangeReadLimiter::new(2),
        1,
    )
    .await
    .unwrap()
    .0
}

#[tokio::test]
async fn joint_routes_use_four_shards_with_or_without_manifest_geometry() {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let router = Router::new(
        MetricType::L2,
        1,
        (0..64).map(|i| i as f32 * 10.0).collect(),
    )
    .unwrap();
    let bytes = router.encode("test-router").unwrap();
    let router_entry = entry(Role::Router(64), bytes.len(), 0, None);
    store(&io, &router_entry, bytes).await;
    let mut entries = vec![router_entry];
    let mut legacy = entries.clone();
    for id in 0..64 {
        let base = if id < 4 { 100.0 + id as f32 } else { 0.0 };
        let (bytes, count) = if id < 8 {
            (flat_shard(64, base, id * ROWS, 3), ROWS)
        } else {
            (centroid_index::EMPTY_SHARD.to_vec(), 0)
        };
        let shard = entry(
            Role::Shard(id),
            bytes.len(),
            count,
            Some(br#"{"localNlist":"64","metric":"l2"}"#),
        );
        store(&io, &shard, bytes).await;
        legacy.push(if id == 0 {
            entry(
                Role::Shard(id),
                shard.index_file.file_size as usize,
                count,
                None,
            )
        } else {
            shard.clone()
        });
        entries.push(shard);
    }
    let joint = run(&io, &entries, HashMap::new()).await;
    let legacy_joint = run(&io, &legacy, HashMap::new()).await;
    assert_eq!(joint[0].row_ids.len(), 10);
    assert!(joint[0].row_ids.iter().all(|&id| id < (4 * ROWS) as u64));
    for (expected, actual) in joint.iter().zip(legacy_joint) {
        assert_eq!(expected.row_ids, actual.row_ids);
        assert_eq!(expected.scores, actual.scores);
    }

    let standard = run(
        &io,
        &entries,
        HashMap::from([("centroid.nprobe".into(), "8".into())]),
    )
    .await;
    assert!(standard[0]
        .row_ids
        .iter()
        .all(|&id| id >= (4 * ROWS) as u64));

    let bytes = flat_shard(1, 100.0, 0, ROWS);
    legacy[1] = entry(Role::Shard(0), bytes.len(), ROWS, None);
    store(&io, &legacy[1], bytes).await;
    let fallback = run(&io, &legacy, HashMap::new()).await;
    assert_eq!(standard[0].row_ids, fallback[0].row_ids);
    assert_eq!(standard[0].scores, fallback[0].scores);
}
