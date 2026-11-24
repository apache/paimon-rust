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
use paimon_vindex_core::io::PosWriter;
use paimon_vindex_core::ivfflat::IVFFlatIndex;
use paimon_vindex_core::ivfflat_io::write_ivfflat_index;
use std::io::Cursor;

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
    let mut bytes = Vec::new();
    write_ivfflat_index(&index, &mut PosWriter::new(&mut bytes)).unwrap();
    bytes
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
    let probes = [Probe::Joint, Probe::Standard, Probe::Joint];
    let result = search_file(&mut reader, &queries, &probes, Cursor::new(bytes), |_| {
        Ok(())
    })
    .unwrap();
    let joint = result[0].as_ref().unwrap();
    assert_eq!(joint.len(), 16);
    assert!((0..16).all(|id| joint.contains_key(&id)));
    assert_eq!(result[1].as_ref().unwrap().len(), 1);
    let retried = result[2].as_ref().unwrap();
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
                index_meta: Some(Descriptor { role, payload }.encode().unwrap()),
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
    queries: &[f32],
    options: HashMap<String, String>,
) -> Vec<ScoredRowIds> {
    let queries = queries
        .iter()
        .copied()
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
            row_range: None,
        },
        VectorIndexWork {
            entry: entries[0].clone(),
            centroid_files: entries.to_vec(),
            bitmap: None,
            queries,
            resolved: Some(options),
            filter: None,
            top_ks: vec![],
        },
        VectorMetricCheck {
            state: Default::default(),
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
async fn joint_routes_use_four_shards_only_with_matching_local_nlists() {
    let io = FileIOBuilder::new("memory").build().unwrap();
    // Keep center IDs in the same order as the handcrafted shards.
    let mut bytes = b"PVROUT01".to_vec();
    for word in [1, 64, MetricType::L2 as u32] {
        bytes.extend(word.to_le_bytes());
    }
    for id in 0..64 {
        bytes.extend((id as f32 * 10.0).to_le_bytes());
    }
    let router_entry = entry(Role::Router(64), bytes.len(), 0, Some(b"l2"));
    store(&io, &router_entry, bytes).await;
    let mut entries = vec![router_entry];
    for id in 0..8 {
        let base = if id < 4 { 100.0 + id as f32 } else { 0.0 };
        let bytes = flat_shard(64, base, id * ROWS, 3);
        let payload = 64i32.to_be_bytes();
        let shard = entry(Role::Shard(id), bytes.len(), ROWS, Some(&payload));
        store(&io, &shard, bytes).await;
        entries.push(shard);
    }
    let explicit = run(
        &io,
        &entries,
        &[630.0],
        HashMap::from([("centroid.nprobe".into(), "1".into())]),
    )
    .await;
    assert!(explicit[0].row_ids.is_empty());
    let widened = run(&io, &entries, &[630.0], HashMap::new()).await;
    assert_eq!(widened[0].row_ids.len(), 10);

    let queries = [0.0, 70.0];
    let joint = run(&io, &entries, &queries, HashMap::new()).await;
    assert_eq!(joint[0].row_ids.len(), 10);
    assert!(joint[0].row_ids.iter().all(|&id| id < (4 * ROWS) as u64));

    let standard = run(
        &io,
        &entries,
        &queries,
        HashMap::from([("centroid.nprobe".into(), "8".into())]),
    )
    .await;
    assert!(standard[0]
        .row_ids
        .iter()
        .all(|&id| id >= (4 * ROWS) as u64));

    let mixed_queries = [0.0, 40.0];
    // Changing shard 4's local list count disqualifies only the second query.
    let bytes = flat_shard(8, 0.0, 4 * ROWS, ROWS / 8);
    let smaller = entry(Role::Shard(4), bytes.len(), ROWS, Some(&8i32.to_be_bytes()));
    store(&io, &smaller, bytes).await;
    entries[5] = smaller;
    let actual = run(&io, &entries, &mixed_queries, HashMap::new()).await;
    let fallback = run(
        &io,
        &entries,
        &mixed_queries,
        HashMap::from([("centroid.nprobe".into(), "8".into())]),
    )
    .await;
    assert_eq!(actual[0].row_ids, joint[0].row_ids);
    assert_eq!(actual[0].scores, joint[0].scores);
    assert_eq!(actual[1].row_ids, fallback[1].row_ids);
    assert_eq!(actual[1].scores, fallback[1].scores);
}
