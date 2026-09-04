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

mod common;

use common::incremental_helpers::{
    make_batch, make_partitioned_batch, memory_table, partitioned_pk_schema, persist_table_schema,
    pk_schema, setup_dirs, write_batch, write_partitioned,
};
use paimon::spec::{CommitKind, Datum, PredicateBuilder, Snapshot};
use paimon::table::{
    IncrementalScanMode, IncrementalSplit, StreamPlan, StreamScanFollowUpMode, StreamScanPoll,
    StreamScanStartupMode,
};

async fn commit_metadata_snapshot(table: &paimon::Table, snapshot_id: i64, kind: CommitKind) {
    let snapshot = Snapshot::builder()
        .version(3)
        .id(snapshot_id)
        .schema_id(table.schema().id())
        .base_manifest_list(String::new())
        .delta_manifest_list(String::new())
        .commit_user("stream-test".to_string())
        .commit_identifier(snapshot_id)
        .commit_kind(kind)
        .time_millis(snapshot_id as u64)
        .watermark(Some(snapshot_id * 100))
        .build();
    assert!(table
        .snapshot_manager()
        .commit_snapshot(&snapshot)
        .await
        .unwrap());
}

async fn write_batch_at_level(table: &paimon::Table, ids: Vec<i32>, values: Vec<i32>, level: i32) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(ids, values))
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    for message in &mut messages {
        for file in &mut message.new_files {
            file.level = level;
        }
    }
    builder.new_commit().commit(messages).await.unwrap();
}

async fn full_start_levels(table: &paimon::Table) -> Vec<i32> {
    let mut scan = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::LatestFull,
            StreamScanFollowUpMode::Auto,
        )
        .await
        .unwrap();
    let plan = expect_data(scan.poll_next().await.unwrap());
    let mut levels = plan
        .full_plan()
        .unwrap()
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .map(|file| file.level)
        .collect::<Vec<_>>();
    levels.sort_unstable();
    levels
}

fn expect_data(poll: StreamScanPoll) -> StreamPlan {
    match poll {
        StreamScanPoll::Data(plan) => plan,
        other => panic!("expected stream data, got {other:?}"),
    }
}

#[tokio::test]
async fn latest_full_is_owned_and_then_follows_new_delta_snapshots() {
    let table_path = "memory:/stream_scan/latest_full";
    let (file_io, table) = memory_table(table_path, partitioned_pk_schema("1"));
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_partitioned(
        &table,
        make_partitioned_batch(vec!["a", "b"], vec![1, 2], vec![10, 20]),
    )
    .await;

    let writer_table = table.clone();
    let mut builder = table.new_read_builder();
    let filter = PredicateBuilder::new(table.schema().fields())
        .equal("pt", Datum::String("a".to_string()))
        .unwrap();
    builder.with_filter(filter);
    builder.with_projection(&["pt", "id"]).unwrap();
    let mut scan = builder
        .new_stream_scan(
            StreamScanStartupMode::LatestFull,
            StreamScanFollowUpMode::Auto,
        )
        .await
        .unwrap();
    drop(builder);
    drop(table);

    let first = expect_data(scan.poll_next().await.unwrap());
    assert_eq!(first.snapshot_id(), 1);
    assert_eq!(first.next_snapshot_id(), 2);
    assert_eq!(scan.checkpoint(), Some(2));
    assert!(matches!(first, StreamPlan::Full { .. }));
    assert_eq!(first.full_plan().unwrap().splits().len(), 1);
    assert_eq!(scan.follow_up_mode(), IncrementalScanMode::Delta);

    write_partitioned(
        &writer_table,
        make_partitioned_batch(vec!["a", "b"], vec![3, 4], vec![30, 40]),
    )
    .await;
    let second = expect_data(scan.poll_next().await.unwrap());
    assert_eq!(second.snapshot_id(), 2);
    assert_eq!(second.next_snapshot_id(), 3);
    assert_eq!(scan.checkpoint(), Some(3));
    let incremental = second.incremental_plan().unwrap();
    assert_eq!(incremental.mode(), IncrementalScanMode::Delta);
    assert_eq!(incremental.splits().len(), 1);

    assert!(matches!(
        scan.poll_next().await.unwrap(),
        StreamScanPoll::Waiting
    ));
}

#[tokio::test]
async fn changelog_full_start_uses_java_level_filters() {
    let lookup_path = "memory:/stream_scan/lookup_full_levels";
    let (lookup_io, lookup_table) =
        memory_table(lookup_path, pk_schema(&[("changelog-producer", "lookup")]));
    setup_dirs(&lookup_io, lookup_path).await;
    persist_table_schema(&lookup_io, lookup_path, lookup_table.schema()).await;
    write_batch_at_level(&lookup_table, vec![1], vec![10], 0).await;
    write_batch_at_level(&lookup_table, vec![2], vec![20], 1).await;
    assert_eq!(full_start_levels(&lookup_table).await, vec![1]);

    let full_compaction_path = "memory:/stream_scan/full_compaction_levels";
    let (full_compaction_io, full_compaction_table) = memory_table(
        full_compaction_path,
        pk_schema(&[
            ("changelog-producer", "full-compaction"),
            ("num-levels", "3"),
        ]),
    );
    setup_dirs(&full_compaction_io, full_compaction_path).await;
    persist_table_schema(
        &full_compaction_io,
        full_compaction_path,
        full_compaction_table.schema(),
    )
    .await;
    write_batch_at_level(&full_compaction_table, vec![1], vec![10], 0).await;
    write_batch_at_level(&full_compaction_table, vec![2], vec![20], 1).await;
    write_batch_at_level(&full_compaction_table, vec![3], vec![30], 2).await;
    assert_eq!(full_start_levels(&full_compaction_table).await, vec![2]);
}

#[tokio::test]
async fn explicit_follow_up_validation_rejects_only_unsafe_combinations() {
    let input_path = "memory:/stream_scan/input_explicit_delta";
    let (input_io, input_table) = memory_table(
        input_path,
        pk_schema(&[("changelog-producer", "input"), ("bucket", "1")]),
    );
    setup_dirs(&input_io, input_path).await;
    persist_table_schema(&input_io, input_path, input_table.schema()).await;
    let scan = input_table
        .new_read_builder()
        .new_stream_scan(StreamScanStartupMode::Latest, StreamScanFollowUpMode::Delta)
        .await
        .expect("input changelog tables may be consumed explicitly as delta");
    assert_eq!(scan.follow_up_mode(), IncrementalScanMode::Delta);

    let dv_lookup_path = "memory:/stream_scan/dv_lookup_explicit_delta";
    let (dv_lookup_io, dv_lookup_table) = memory_table(
        dv_lookup_path,
        pk_schema(&[
            ("changelog-producer", "lookup"),
            ("deletion-vectors.enabled", "true"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&dv_lookup_io, dv_lookup_path).await;
    persist_table_schema(&dv_lookup_io, dv_lookup_path, dv_lookup_table.schema()).await;
    let error = dv_lookup_table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::LatestFull,
            StreamScanFollowUpMode::Delta,
        )
        .await
        .expect_err("DV lookup requires future compaction changelog");
    assert!(matches!(error, paimon::Error::Unsupported { .. }));
    let scan = dv_lookup_table
        .new_read_builder()
        .new_stream_scan(StreamScanStartupMode::Latest, StreamScanFollowUpMode::Delta)
        .await
        .expect("non-full startup may explicitly consume future lookup deltas");
    assert_eq!(scan.follow_up_mode(), IncrementalScanMode::Delta);

    let none_path = "memory:/stream_scan/none_explicit_changelog";
    let (none_io, none_table) = memory_table(
        none_path,
        pk_schema(&[("changelog-producer", "none"), ("bucket", "1")]),
    );
    setup_dirs(&none_io, none_path).await;
    persist_table_schema(&none_io, none_path, none_table.schema()).await;
    let error = none_table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::Latest,
            StreamScanFollowUpMode::Changelog,
        )
        .await
        .expect_err("a table without changelog files cannot use changelog follow-up");
    assert!(matches!(error, paimon::Error::Unsupported { .. }));
}

#[tokio::test]
async fn deletion_vector_full_start_replays_starting_level_zero() {
    let table_path = "memory:/stream_scan/dv_full_start";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("deletion-vectors.enabled", "true"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    for startup in [
        StreamScanStartupMode::LatestFull,
        StreamScanStartupMode::FromSnapshotFull(1),
    ] {
        let mut scan = table
            .new_read_builder()
            .new_stream_scan(startup, StreamScanFollowUpMode::Auto)
            .await
            .unwrap();
        let full = expect_data(scan.poll_next().await.unwrap());
        assert!(full.full_plan().unwrap().splits().is_empty());
        assert_eq!(full.next_snapshot_id(), 1);
        assert_eq!(scan.checkpoint(), Some(1));

        let incremental = expect_data(scan.poll_next().await.unwrap());
        assert_eq!(incremental.snapshot_id(), 1);
        assert_eq!(incremental.next_snapshot_id(), 2);
        let splits = incremental.incremental_plan().unwrap().splits();
        assert!(!splits.is_empty());
        for split in splits {
            let IncrementalSplit::Data(split) = split else {
                panic!("stream delta must contain data splits");
            };
            assert!(split.data_files().iter().all(|file| file.level == 0));
            assert!(split.data_deletion_files().is_none());
        }
    }
}

#[tokio::test]
async fn latest_on_initially_empty_table_includes_first_snapshot() {
    let table_path = "memory:/stream_scan/latest_empty";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[("changelog-producer", "none"), ("bucket", "1")]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let mut scan = table
        .new_read_builder()
        .new_stream_scan(StreamScanStartupMode::Latest, StreamScanFollowUpMode::Delta)
        .await
        .unwrap();
    assert_eq!(scan.checkpoint(), Some(1));

    // Commit after construction but before the first poll. The async
    // constructor freezes the empty-table boundary, so snapshot 1 is retained.
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let first = expect_data(scan.poll_next().await.unwrap());
    assert_eq!(first.snapshot_id(), 1);
    assert_eq!(scan.checkpoint(), Some(2));
}

#[tokio::test]
async fn inconsistent_range_observation_recovers_across_polls() {
    let table_path = "memory:/stream_scan/transient_range";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[("changelog-producer", "none"), ("bucket", "1")]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    // Model a commit between earliest/latest observations: the latest hint is
    // visible while the corresponding snapshot is not yet visible.
    table.snapshot_manager().write_latest_hint(1).await.unwrap();
    let mut scan = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshot(1),
            StreamScanFollowUpMode::Delta,
        )
        .await
        .unwrap();
    assert!(matches!(
        scan.poll_next().await.unwrap(),
        StreamScanPoll::Waiting
    ));

    commit_metadata_snapshot(&table, 1, CommitKind::APPEND).await;
    assert!(matches!(
        scan.poll_next().await.unwrap(),
        StreamScanPoll::Waiting
    ));
    assert!(matches!(
        scan.poll_next().await.unwrap(),
        StreamScanPoll::Waiting
    ));
    assert_eq!(scan.checkpoint(), Some(2));
}

#[tokio::test]
async fn empty_append_snapshot_advances_cursor_and_restore_replays_from_checkpoint() {
    let table_path = "memory:/stream_scan/empty_snapshot";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[("changelog-producer", "none"), ("bucket", "1")]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    commit_metadata_snapshot(&table, 2, CommitKind::APPEND).await;

    let mut scan = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshot(1),
            StreamScanFollowUpMode::Delta,
        )
        .await
        .unwrap();
    assert!(matches!(
        scan.poll_next().await.unwrap(),
        StreamScanPoll::Waiting
    ));
    assert_eq!(scan.checkpoint(), Some(1));
    assert_eq!(
        expect_data(scan.poll_next().await.unwrap()).snapshot_id(),
        1
    );

    // Snapshot 2 is an APPEND with an empty delta manifest. One poll consumes
    // it and waits for snapshot 3 instead of returning the same empty plan.
    assert!(matches!(
        scan.poll_next().await.unwrap(),
        StreamScanPoll::Waiting
    ));
    assert_eq!(scan.checkpoint(), Some(3));
    assert_eq!(scan.watermark(), Some(200));

    scan.restore(Some(1)).unwrap();
    assert_eq!(
        expect_data(scan.poll_next().await.unwrap()).snapshot_id(),
        1
    );
    assert_eq!(scan.checkpoint(), Some(2));
}

#[tokio::test]
async fn transient_missing_snapshot_waits_but_range_errors_are_explicit() {
    let table_path = "memory:/stream_scan/range_errors";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[("changelog-producer", "none"), ("bucket", "1")]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    commit_metadata_snapshot(&table, 3, CommitKind::APPEND).await;

    let mut missing = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshot(2),
            StreamScanFollowUpMode::Delta,
        )
        .await
        .unwrap();
    // Poll frequency must not turn an eventually consistent in-range miss into
    // a permanent gap. It remains Waiting until the object becomes visible.
    for _ in 0..16 {
        assert!(matches!(
            missing.poll_next().await.unwrap(),
            StreamScanPoll::Waiting
        ));
    }

    table.snapshot_manager().delete_snapshot(1).await.unwrap();
    let mut expired = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshot(1),
            StreamScanFollowUpMode::Delta,
        )
        .await
        .unwrap();
    let expired_error = expired.poll_next().await.unwrap_err();
    assert!(matches!(expired_error, paimon::Error::DataInvalid { .. }));
    assert!(expired_error.to_string().contains("expired"));

    let mut too_large = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshot(5),
            StreamScanFollowUpMode::Delta,
        )
        .await
        .unwrap();
    let too_large_error = too_large.poll_next().await.unwrap_err();
    assert!(matches!(too_large_error, paimon::Error::DataInvalid { .. }));
    assert!(too_large_error.to_string().contains("too large"));
}

#[tokio::test]
async fn overwrite_follow_up_is_not_silently_skipped() {
    let table_path = "memory:/stream_scan/overwrite";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[("changelog-producer", "none"), ("bucket", "1")]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    let mut scan = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshot(1),
            StreamScanFollowUpMode::Auto,
        )
        .await
        .unwrap();
    assert!(matches!(
        scan.poll_next().await.unwrap(),
        StreamScanPoll::Waiting
    ));
    assert_eq!(
        expect_data(scan.poll_next().await.unwrap()).snapshot_id(),
        1
    );
    commit_metadata_snapshot(&table, 2, CommitKind::OVERWRITE).await;

    let error = scan.poll_next().await.unwrap_err();
    assert!(matches!(error, paimon::Error::Unsupported { .. }));
    assert!(error.to_string().contains("OVERWRITE snapshot 2"));
    assert_eq!(scan.checkpoint(), Some(2));
}

#[tokio::test]
async fn from_snapshot_full_reads_exact_snapshot_and_reports_missing_target() {
    let table_path = "memory:/stream_scan/from_full";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[("changelog-producer", "none"), ("bucket", "1")]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    let mut full = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshotFull(1),
            StreamScanFollowUpMode::Delta,
        )
        .await
        .unwrap();
    let plan = expect_data(full.poll_next().await.unwrap());
    assert!(matches!(plan, StreamPlan::Full { .. }));
    assert_eq!(plan.snapshot_id(), 1);
    assert_eq!(full.checkpoint(), Some(2));

    let mut missing = table
        .new_read_builder()
        .new_stream_scan(
            StreamScanStartupMode::FromSnapshotFull(2),
            StreamScanFollowUpMode::Delta,
        )
        .await
        .unwrap();
    assert!(matches!(
        missing.poll_next().await.unwrap_err(),
        paimon::Error::SnapshotNotExist { snapshot_id: 2 }
    ));
}
