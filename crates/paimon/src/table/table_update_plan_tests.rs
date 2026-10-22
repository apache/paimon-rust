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
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, IntType, Schema, TableSchema};
use crate::table::{DataSplitBuilder, Plan, RowRange};
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use std::sync::Arc;

async fn table(name: &str) -> Table {
    let schema = Schema::builder()
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", "-1")
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", name),
        format!("memory:/{name}"),
        TableSchema::new(0, &schema),
        None,
    );
    for value in [10, 20] {
        append(&table, value).await;
    }
    table
}

async fn append(table: &Table, value: i32) {
    let batch = RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(Int32Array::from(vec![value])) as ArrayRef,
    )])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

fn update_batch(row_id: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "_ROW_ID",
            Arc::new(Int64Array::from(vec![row_id])) as ArrayRef,
        ),
        ("value", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
    ])
    .unwrap()
}

#[tokio::test]
async fn planned_updates_and_deletes_keep_the_captured_baseline_without_manifest_rescan() {
    let table = table("planned_mutation_baseline").await;
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    assert_eq!(plan.snapshot_id(), Some(2));
    append(&table, 30).await;
    table
        .file_io()
        .delete_dir(&format!("{}/manifest", table.location()))
        .await
        .unwrap();

    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_plan(plan).unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![update_batch(0)])
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].check_from_snapshot, Some(2));
    let deletes = update.delete_by_row_id(vec![1]).await.unwrap();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].check_from_snapshot, Some(2));
    let error = update
        .update_by_arrow_with_row_id(vec![update_batch(2)])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("No file found for _ROW_ID 2"));
}

#[tokio::test]
async fn planned_mutations_reject_foreign_paths_and_selected_splits_before_writing() {
    let table = table("planned_mutation_validation").await;
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    let split = &plan.splits()[0];
    for (path, snapshot_id, ranges, streaming) in [
        ("memory:/another/bucket-0", 2, None, false),
        (split.bucket_path(), 1, None, false),
        (
            split.bucket_path(),
            2,
            Some(vec![RowRange::new(0, 0)]),
            false,
        ),
        (split.bucket_path(), 2, None, true),
    ] {
        let mut builder = DataSplitBuilder::new()
            .with_snapshot(snapshot_id)
            .with_partition(split.partition().clone())
            .with_bucket(split.bucket())
            .with_bucket_path(path.to_string())
            .with_data_files(split.data_files().to_vec())
            .with_streaming(streaming);
        if let Some(ranges) = ranges {
            builder = builder.with_row_ranges(ranges);
        }
        let split = builder.build().unwrap();
        let mut update = table.new_write_builder().new_update().unwrap();
        if update
            .with_plan(Plan::from_splits(Some(2), vec![split]))
            .is_err()
        {
            continue;
        }
        assert!(update.new_update_by_row_id().await.is_err());
        assert!(update.delete_by_row_id(vec![0]).await.is_err());
    }
}

#[tokio::test]
async fn planned_update_rejects_captured_deleted_rows_but_allows_live_rows() {
    let table = table("planned_update_deletion_vector").await;
    let update = table.new_write_builder().new_update().unwrap();
    let deletes = update.delete_by_row_id(vec![0]).await.unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(deletes)
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    assert!(plan
        .splits()
        .iter()
        .any(|split| split.data_deletion_files().is_some()));
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_plan(plan).unwrap();
    let mut writer = update.new_update_by_row_id().await.unwrap();
    let error = writer
        .update_columns(vec![update_batch(0)], vec!["value".into()])
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Cannot update deleted _ROW_ID 0"));
    assert!(writer.commit_messages().is_empty());
    let messages = writer
        .update_columns(vec![update_batch(1)], vec!["value".into()])
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].check_from_snapshot, Some(3));
}

#[tokio::test]
async fn row_id_plan_routes_complete_groups_and_rejects_ambiguous_ranges() {
    let table = table("planned_row_id_routing").await;
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    let routing = crate::table::RowIdPlan::from_plan(plan.clone()).unwrap();
    assert_eq!(routing.snapshot_id(), Some(2));
    assert_eq!(
        routing
            .groups()
            .iter()
            .map(|(first, last, _)| (*first, *last))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 1)]
    );
    let selected = routing.groups()[0].2.clone();
    let mut update = table.new_write_builder().new_update().unwrap();
    update
        .with_plan(Plan::from_splits(Some(2), vec![selected.clone()]))
        .unwrap();
    assert!(update
        .delete_by_row_id(vec![1])
        .await
        .unwrap_err()
        .to_string()
        .contains("No file found for _ROW_ID 1"));
    assert!(crate::table::RowIdPlan::from_plan(Plan::from_splits(
        Some(2),
        vec![selected.clone(), selected]
    ))
    .unwrap_err()
    .to_string()
    .contains("overlapping file groups"));
    assert_eq!(routing.plan().splits(), plan.splits());
}

#[tokio::test]
async fn row_id_plan_keeps_full_partial_columns_and_dedicated_subranges() {
    let table = table("planned_row_id_partial_ranges").await;
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    let split = &plan.splits()[0];
    let mut base = split.data_files()[0].clone();
    base.first_row_id = Some(0);
    base.row_count = 6;
    base.max_sequence_number = 0;
    let mut partial = base.clone();
    partial.file_name = "partial-column.parquet".into();
    partial.max_sequence_number = 1;
    partial.write_cols = Some(vec!["value".into()]);
    let mut left = partial.clone();
    left.file_name = "left.blob".into();
    left.row_count = 2;
    left.max_sequence_number = 1;
    left.write_cols = Some(vec!["value".into()]);
    let mut right = left.clone();
    right.file_name = "right.vector.store".into();
    right.first_row_id = Some(2);
    right.row_count = 4;
    let files = vec![right, base.clone(), left.clone(), partial];
    let split = split.for_data_evolution_files(files.clone());
    let routing =
        crate::table::RowIdPlan::from_plan(Plan::from_splits(Some(2), vec![split.clone()]))
            .unwrap();
    assert_eq!(routing.groups().len(), 1);
    assert_eq!((routing.groups()[0].0, routing.groups()[0].1), (0, 5));
    assert_eq!(routing.groups()[0].2.data_files().len(), 4);
    assert!(files
        .iter()
        .all(|file| routing.groups()[0].2.data_files().contains(file)));
    left.file_name = "invalid-short-column.parquet".into();
    let invalid = split.for_data_evolution_files(vec![base, left]);
    assert!(
        crate::table::RowIdPlan::from_plan(Plan::from_splits(Some(2), vec![invalid]))
            .unwrap_err()
            .to_string()
            .contains("identical ranges")
    );
}

#[tokio::test]
async fn captured_plan_rejects_other_operation_families_before_io_or_callbacks() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let table = table("planned_operation_family").await;
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_plan(plan).unwrap();
    for directory in ["manifest", "snapshot"] {
        table
            .file_io()
            .delete_dir(&format!("{}/{directory}", table.location()))
            .await
            .unwrap();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let callback = crate::table::UpdateAssignment::Function(Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }));
    let errors = vec![
        update
            .new_shard_updater(0, 1, None)
            .await
            .err()
            .unwrap()
            .to_string(),
        update
            .update_by_predicate(None, vec![("value".into(), callback)], vec![])
            .await
            .unwrap_err()
            .to_string(),
        update
            .upsert_by_arrow_with_key(vec![], vec!["value".into()])
            .await
            .unwrap_err()
            .to_string(),
        update
            .merge_into(crate::table::MergeSource::SelfTable, vec![], vec![], vec![])
            .await
            .unwrap_err()
            .to_string(),
        update
            .delete_by_predicate(None)
            .await
            .unwrap_err()
            .to_string(),
    ];
    assert!(
        errors
            .iter()
            .all(|error| error.contains("with_plan supports only row-ID")),
        "{errors:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
