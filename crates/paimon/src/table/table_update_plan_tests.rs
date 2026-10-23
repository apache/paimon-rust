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
use crate::table::{
    DataSplit, DataSplitBuilder, DeletionFile, Plan, RowIdPlan, RowRange, ScanStats,
};
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
async fn planned_delete_preserves_unselected_bucket_deletion_vectors() {
    use futures::TryStreamExt;

    let table = table("planned_delete_bucket_deletion_vectors").await;
    let deletes = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .delete_by_row_id(vec![0])
        .await
        .unwrap();
    let previous_index = deletes[0].new_index_files[0].clone();
    let previous_anchor = previous_index
        .deletion_vectors_ranges
        .as_ref()
        .unwrap()
        .keys()
        .next()
        .unwrap()
        .clone();
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
    let routing = crate::table::RowIdPlan::from_plan(plan.clone()).unwrap();
    // Route only the second anchor. The first anchor's existing deletion
    // vector must survive replacement of the bucket's shared index file.
    let selected = routing
        .groups()
        .iter()
        .find(|(first, _, _)| *first == 1)
        .unwrap()
        .2
        .clone();
    let mut update = table.new_write_builder().new_update().unwrap();
    update
        .with_plan(Plan::from_splits(plan.snapshot_id(), vec![selected]))
        .unwrap();
    let messages = update.delete_by_row_id(vec![1]).await.unwrap();
    assert_eq!(messages[0].check_from_snapshot, Some(3));
    assert_eq!(messages[0].deleted_index_files, vec![previous_index]);
    let ranges = messages[0].new_index_files[0]
        .deletion_vectors_ranges
        .as_ref()
        .unwrap();
    assert_eq!(ranges.len(), 2);
    assert_eq!(ranges[&previous_anchor].cardinality, Some(1));
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
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

fn routing_split(
    source: &DataSplit,
    first: i64,
    count: i64,
    partition: i32,
    bucket: i32,
) -> DataSplit {
    let mut file = source.data_files()[0].clone();
    file.file_name = format!("anchor-{first}.parquet");
    file.first_row_id = Some(first);
    file.row_count = count;
    file.write_cols = None;
    let mut partition_row = crate::spec::BinaryRowBuilder::new(1);
    partition_row.write_int(0, partition);
    DataSplitBuilder::new()
        .with_snapshot(source.snapshot_id())
        .with_partition(partition_row.build())
        .with_bucket(bucket)
        .with_bucket_path(format!("memory:/partition-{partition}/bucket-{bucket}"))
        .with_data_files(vec![file])
        .build()
        .unwrap()
}

#[tokio::test]
async fn row_id_plan_routes_sliced_ids_by_anchor_and_partition_bucket() {
    let table = table("row_id_arrow_routing").await;
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    let source = &plan.splits()[0];
    let splits = vec![
        routing_split(source, 30, 5, 1, 1),
        routing_split(source, i64::MAX - 1, 2, 1, 0),
        routing_split(source, 5, 4, 1, 0),
        routing_split(source, 20, 2, 2, 0),
        routing_split(source, 0, 3, 1, 0),
    ];
    let routing = RowIdPlan::from_plan(Plan::from_splits(Some(2), splits)).unwrap();
    // Slices, repeated IDs and arbitrary order must retain their coordinates.
    // The same bucket number in another partition must remain independent.
    let ids =
        Int64Array::from(vec![-99, i64::MAX, 8, 20, 2, 30, 5, i64::MAX - 1, 8, -99]).slice(1, 8);
    assert_eq!(
        routing.route(&ids).unwrap().values(),
        &[i64::MAX - 1, 5, 20, 0, 30, 5, i64::MAX - 1, 5]
    );
    assert_eq!(
        routing.route_by_bucket(&ids).unwrap().values(),
        &[0, 0, 20, 0, 30, 0, 0, 0]
    );
    // Bucket representatives come from the complete plan, not only the IDs
    // present in this input batch or the order of the input splits.
    assert_eq!(
        routing
            .route_by_bucket(&Int64Array::from(vec![7]))
            .unwrap()
            .values(),
        &[0]
    );
    let mut reversed = routing.plan().splits().to_vec();
    reversed.reverse();
    let reordered = RowIdPlan::from_plan(Plan::from_splits(Some(2), reversed)).unwrap();
    assert_eq!(routing.route(&ids).unwrap(), reordered.route(&ids).unwrap());
    assert_eq!(
        routing.route_by_bucket(&ids).unwrap(),
        reordered.route_by_bucket(&ids).unwrap()
    );

    for invalid_id in [-1, 3, 4, 9, 19, 22, 35] {
        let ids = Int64Array::from(vec![0, invalid_id]);
        for error in [
            routing.route(&ids).unwrap_err(),
            routing.route_by_bucket(&ids).unwrap_err(),
            routing.select(&ids).unwrap_err(),
        ] {
            assert!(error.to_string().contains(&format!(
                "_ROW_ID {invalid_id} does not belong to any valid range"
            )));
        }
    }
    let null_ids = Int64Array::from(vec![Some(0), None]);
    for error in [
        routing.route(&null_ids).unwrap_err(),
        routing.route_by_bucket(&null_ids).unwrap_err(),
        routing.select(&null_ids).unwrap_err(),
    ] {
        assert!(error.to_string().contains("_ROW_ID must not be null"));
    }
    // A failed routing request never changes the captured metadata.
    assert_eq!(routing.route(&ids).unwrap().len(), ids.len());
}

#[test]
fn row_id_plan_empty_inputs_keep_the_captured_snapshot() {
    for snapshot_id in [None, Some(7)] {
        let routing = RowIdPlan::from_plan(Plan::from_splits(snapshot_id, Vec::new())).unwrap();
        let empty = Int64Array::from(Vec::<i64>::new());
        assert!(routing.route(&empty).unwrap().is_empty());
        assert!(routing.route_by_bucket(&empty).unwrap().is_empty());
        let selected = routing.select(&empty).unwrap();
        assert_eq!(selected.snapshot_id(), snapshot_id);
        assert!(selected.splits().is_empty());
        assert!(routing.route(&Int64Array::from(vec![0])).is_err());
        assert!(routing.route_by_bucket(&Int64Array::from(vec![0])).is_err());
        assert!(routing.select(&Int64Array::from(vec![0])).is_err());
        assert!(routing
            .select(&Int64Array::from(vec![None::<i64>]))
            .is_err());
    }
}

#[tokio::test]
async fn row_id_plan_read_selection_retains_complete_groups_paths_deletions_and_grants() {
    let table = table("row_id_read_selection").await;
    let initial = table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap();
    let source = &initial.splits()[0];
    let mut first = routing_split(source, 10, 6, 1, 0);
    let mut anchor = first.data_files()[0].clone();
    anchor.external_path = Some("s3://external/anchor-10.parquet".into());
    let mut partial = anchor.clone();
    partial.file_name = "partial-column.parquet".into();
    partial.external_path = Some("s3://external/partial-column.parquet".into());
    partial.max_sequence_number += 1;
    partial.write_cols = Some(vec!["value".into()]);
    let mut blob = partial.clone();
    blob.file_name = "partial-payload.blob".into();
    blob.external_path = Some("s3://external/partial-payload.blob".into());
    blob.first_row_id = Some(11);
    blob.row_count = 2;
    let deletions = vec![
        None,
        Some(DeletionFile::new(
            "s3://external/deletions".into(),
            5,
            20,
            Some(1),
        )),
        None,
    ];
    first = DataSplitBuilder::new()
        .with_snapshot(2)
        .with_partition(first.partition().clone())
        .with_bucket(first.bucket())
        .with_bucket_path(first.bucket_path().to_string())
        .with_data_files(vec![blob, anchor, partial])
        .with_data_deletion_files(deletions)
        .with_raw_convertible(false)
        .build()
        .unwrap();
    let grant = Arc::new(
        crate::table::query_auth::QueryAuthGrant::parse(
            &crate::api::AuthTableQueryResponse::default(),
            17,
            None,
            table.schema().fields(),
        )
        .unwrap(),
    );
    let stats = ScanStats {
        manifest_entries_read: 123,
        split_candidates_built: 3,
        ..Default::default()
    };
    let plan = Plan::from_splits(
        Some(2),
        vec![
            routing_split(source, 60, 2, 1, 0),
            first,
            routing_split(source, 30, 5, 1, 0),
        ],
    )
    .planned(Some(grant.clone()))
    .with_scan_stats(stats);
    let routing = RowIdPlan::from_plan(plan).unwrap();
    let ids = Int64Array::from(vec![999, 15, 10, 12, 12, 30, 13, 34, 11, 999]).slice(1, 8);
    let selected = routing.select(&ids).unwrap();
    assert_eq!(selected.snapshot_id(), Some(2));
    assert_eq!(selected.splits().len(), 2);
    assert_eq!(selected.stats().manifest_entries_read, 123);
    assert_eq!(selected.stats().split_candidates_built, 3);
    assert_eq!(selected.stats().final_splits, 2);
    assert_eq!(selected.stats().final_files, 4);
    assert_eq!(
        selected.splits()[0].row_ranges(),
        Some([RowRange::new(10, 13), RowRange::new(15, 15)].as_slice())
    );
    assert_eq!(
        selected.splits()[1].row_ranges(),
        Some([RowRange::new(30, 30), RowRange::new(34, 34)].as_slice())
    );
    for (selected, (_, _, original)) in selected.splits().iter().zip(routing.groups()) {
        assert_eq!(selected.data_files(), original.data_files());
        assert_eq!(
            selected.data_deletion_files(),
            original.data_deletion_files()
        );
        assert_eq!(selected.partition(), original.partition());
        assert_eq!(selected.bucket(), original.bucket());
        assert_eq!(selected.bucket_path(), original.bucket_path());
        assert_eq!(selected.raw_convertible(), original.raw_convertible());
        assert!(selected.query_auth_required());
        assert!(Arc::ptr_eq(selected.query_auth_grant().unwrap(), &grant));
        assert_eq!(
            selected.query_auth_response(),
            original.query_auth_response()
        );

        // Wire transport preserves the read selection and auth requirement,
        // but cannot turn serialized policy metadata back into a live grant.
        let restored =
            DataSplit::deserialize_split_v1(&selected.serialize_split_v1().unwrap()).unwrap();
        assert_eq!(restored.row_ranges(), selected.row_ranges());
        assert_eq!(restored.data_files(), selected.data_files());
        assert_eq!(
            restored.data_deletion_files(),
            selected.data_deletion_files()
        );
        assert!(restored.query_auth_required());
        assert!(restored.query_auth_grant().is_none());
    }
    assert_eq!(routing.plan().splits().len(), 3);
    assert!(routing
        .groups()
        .iter()
        .all(|(_, _, split)| split.row_ranges().is_none()));
    // A read selection is intentionally not accepted as a mutation baseline.
    assert!(RowIdPlan::from_plan(selected.clone()).is_err());
    assert!(table
        .new_write_builder()
        .new_update()
        .unwrap()
        .with_plan(selected)
        .is_err());

    let empty = routing
        .select(&Int64Array::from(Vec::<i64>::new()))
        .unwrap();
    assert_eq!(empty.snapshot_id(), Some(2));
    assert!(empty.splits().is_empty());
    assert_eq!(empty.stats().manifest_entries_read, 123);
    assert_eq!(empty.stats().final_files, 0);
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
