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
use crate::spec::{DataType, IntType, Schema, TableSchema};
use crate::table::maintenance_test_utils::{source_manifests, table};
use crate::table::Table;
use arrow_array::{ArrayRef, Int32Array, RecordBatch};

async fn captured_sources() -> (
    Table,
    GlobalIndexBuildView,
    Arc<[ManifestEntry]>,
    Arc<crate::table::maintenance_test_utils::MetadataIo>,
) {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("write-only", "true")
        .build()
        .unwrap();
    let (table, counter) = table(TableSchema::new(0, &schema)).await;
    for id in 0..3 {
        let write = table.new_write_builder();
        let mut writer = write.new_write().unwrap();
        writer
            .write_arrow_batch(
                &RecordBatch::try_from_iter([(
                    "id",
                    Arc::new(Int32Array::from(vec![id])) as ArrayRef,
                )])
                .unwrap(),
            )
            .await
            .unwrap();
        write
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let mut entries = crate::table::snapshot_manifest::read_snapshot_entries(
        &table.snapshot_manager(),
        &snapshot,
        None,
    )
    .await
    .unwrap();
    entries.sort_by_key(|entry| entry.file().first_row_id);
    for (id, entry) in entries.iter_mut().enumerate() {
        // Opaque inline index/stat payloads make deep clones materially costly.
        entry.file.embedded_index = Some(vec![id as u8; 32 * 1024]);
    }
    let source: Arc<[ManifestEntry]> = entries.into();
    let view = GlobalIndexBuildView::capture(&table, Some(snapshot.clone()))
        .await
        .unwrap()
        .with_source_entries(source.clone());
    counter.watch(source_manifests(&table, &snapshot).await);
    (table, view, source, counter)
}

#[tokio::test]
async fn source_filters_share_metadata_and_materialize_only_selected_entries() {
    let (table, view, original, counter) = captured_sources().await;
    let filtered = view.filter_sources(|entry| entry.file().first_row_id.unwrap() >= 1);
    assert!(Arc::ptr_eq(filtered.source_entries.as_ref().unwrap(), &original),
        "partial views must share the captured metadata instead of retaining a deep clone during native builds");
    let first = filtered.manifest_entries(&table, None).await.unwrap();
    assert_eq!(
        first
            .iter()
            .map(|entry| entry.file().first_row_id.unwrap())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let twice = filtered
        .filter_sources(|entry| entry.file().first_row_id == Some(2))
        .filter_indexes(|_| true);
    assert!(Arc::ptr_eq(
        twice.source_entries.as_ref().unwrap(),
        &original
    ));
    assert_eq!(
        twice.snapshot().unwrap().id(),
        view.snapshot().unwrap().id()
    );
    let selected = twice.manifest_entries(&table, None).await.unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].file().first_row_id, Some(2));
    assert_eq!(
        selected[0].file().embedded_index.as_deref(),
        Some(vec![2; 32 * 1024].as_slice())
    );
    let empty = twice.filter_sources(|_| false);
    assert!(empty
        .filter_sources(|_| true)
        .manifest_entries(&table, None)
        .await
        .unwrap()
        .is_empty());
    // A refreshed capture owns a new source buffer and must drop old positions.
    let reset = twice.with_source_entries(vec![original[0].clone()].into());
    let reset_entries = reset.manifest_entries(&table, None).await.unwrap();
    assert_eq!(reset_entries.len(), 1);
    assert_eq!(reset_entries[0].file().first_row_id, Some(0));
    assert_eq!(view.manifest_entries(&table, None).await.unwrap().len(), 3);
    assert_eq!(
        filtered.manifest_entries(&table, None).await.unwrap().len(),
        2
    );
    assert!(
        counter.counts().values().all(|reads| *reads == 0),
        "{:?}",
        counter.counts()
    );
}
