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
use arrow_array::{
    ArrayRef, Int32Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use paimon::spec::{BigIntType, LocalZonedTimestampType, TimestampType, VALUE_KIND_FIELD_NAME};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

async fn compact(table: &Table) -> paimon::table::CompactMetrics {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(!plan.tasks().is_empty(), "expected compaction tasks");
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(table, &plan).await)
        .await
        .unwrap()
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn expire_schema(
    time_type: DataType,
    options: &[(&str, &str)],
    merge_engine: &str,
) -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("ts", time_type)
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", merge_engine)
        .option("record-level.expire-time", "1 s")
        .option("record-level.time-field", "ts")
        .option("num-sorted-run.compaction-trigger", "2");
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    TableSchema::new(0, &builder.build().unwrap())
}

fn int_batch(ids: Vec<i32>, ts: Vec<Option<i32>>, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("ts", ArrowDataType::Int32, true),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(ts)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

fn bigint_batch(ids: Vec<i32>, ts: Vec<i64>, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("ts", ArrowDataType::Int64, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

fn timestamp_batch(
    ids: Vec<i32>,
    millis: Vec<i64>,
    values: Vec<i32>,
    unit: TimeUnit,
    tz: Option<&str>,
) -> RecordBatch {
    let tz = tz.map(|zone| Arc::<str>::from(zone));
    let array: ArrayRef = match unit {
        TimeUnit::Millisecond => Arc::new(
            TimestampMillisecondArray::from(millis).with_timezone_opt(tz.clone()),
        ),
        TimeUnit::Microsecond => {
            let micros = millis.into_iter().map(|m| m * 1000).collect::<Vec<_>>();
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone_opt(tz.clone()))
        }
        _ => panic!("unsupported unit"),
    };
    let ts_type = ArrowDataType::Timestamp(unit, tz);
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("ts", ts_type, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            array,
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

async fn id_values(table: &Table) -> Vec<(i32, i32)> {
    let mut rows = Vec::new();
    for batch in read(table, Some(&["id", "value"])).await {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    rows.sort_unstable();
    rows
}

async fn id_value(table: &Table, id: i32) -> Option<i32> {
    id_values(table)
        .await
        .into_iter()
        .find(|(key, _)| *key == id)
        .map(|(_, value)| value)
}

async fn all_id_values(table: &Table) -> Vec<(i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    rows.sort_unstable();
    rows
}

async fn active_files(table: &Table) -> Vec<paimon::spec::DataFileMeta> {
    table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap()
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .cloned()
        .collect()
}

#[tokio::test]
async fn expire_filters_int_seconds_after_compact_but_not_before() {
    let table = table_with_schema(expire_schema(
        DataType::Int(IntType::new()),
        &[],
        "deduplicate",
    ))
    .await;
    let live = i32::try_from(now_secs() + 3600).unwrap();
    write_batch(
        &table,
        &int_batch(vec![1, 2], vec![Some(1), Some(live)], vec![10, 20]),
    )
    .await;
    assert_eq!(id_values(&table).await, vec![(1, 10), (2, 20)]);
    assert_eq!(all_id_values(&table).await, vec![(1, 10), (2, 20)]);
    compact(&table).await;
    assert_eq!(id_values(&table).await, vec![(2, 20)]);
    assert_eq!(all_id_values(&table).await, vec![(2, 20)]);
}

#[tokio::test]
async fn expire_keeps_null_time_and_negative_boundary() {
    let table = table_with_schema(expire_schema(
        DataType::Int(IntType::new()),
        &[],
        "deduplicate",
    ))
    .await;
    // NULL time is always kept. Negative timestamps use the same keep rule.
    write_batch(
        &table,
        &int_batch(vec![1, 2], vec![None, Some(-1)], vec![10, 20]),
    )
    .await;
    compact(&table).await;
    assert_eq!(id_values(&table).await, vec![(1, 10)]);
}

#[tokio::test]
async fn expire_bigint_seconds_and_millis_threshold() {
    let table = table_with_schema(expire_schema(
        DataType::BigInt(BigIntType::new()),
        &[],
        "deduplicate",
    ))
    .await;
    let live_secs = now_secs() + 3600;
    // Below 1e12 → seconds; at/above → millis.
    write_batch(
        &table,
        &bigint_batch(
            vec![1, 2, 3],
            vec![1, live_secs, live_secs * 1000],
            vec![10, 20, 30],
        ),
    )
    .await;
    assert_eq!(id_values(&table).await, vec![(1, 10), (2, 20), (3, 30)]);
    compact(&table).await;
    assert_eq!(id_values(&table).await, vec![(2, 20), (3, 30)]);
}

#[tokio::test]
async fn expire_timestamp_and_timestamp_ltz() {
    for (time_type, unit, tz) in [
        (
            DataType::Timestamp(TimestampType::new(3).unwrap()),
            TimeUnit::Millisecond,
            None,
        ),
        (
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
            TimeUnit::Millisecond,
            Some("UTC"),
        ),
        (
            DataType::Timestamp(TimestampType::new(6).unwrap()),
            TimeUnit::Microsecond,
            None,
        ),
    ] {
        let table = table_with_schema(expire_schema(time_type, &[], "deduplicate")).await;
        let live_millis = (now_secs() + 3600) * 1000;
        write_batch(
            &table,
            &timestamp_batch(vec![1, 2], vec![1_000, live_millis], vec![10, 20], unit, tz),
        )
        .await;
        assert_eq!(id_values(&table).await, vec![(1, 10), (2, 20)]);
        compact(&table).await;
        assert_eq!(id_values(&table).await, vec![(2, 20)], "{unit:?}/{tz:?}");
    }
}

#[tokio::test]
async fn expire_before_aggregation_and_partial_update() {
    let agg = table_with_schema(expire_schema(
        DataType::Int(IntType::new()),
        &[("fields.value.aggregate-function", "sum")],
        "aggregation",
    ))
    .await;
    let live = i32::try_from(now_secs() + 3600).unwrap();
    write_batch(&agg, &int_batch(vec![1], vec![Some(1)], vec![10])).await;
    write_batch(&agg, &int_batch(vec![1], vec![Some(live)], vec![5])).await;
    // Query still merges both versions (expire is compaction-only).
    assert_eq!(id_values(&agg).await, vec![(1, 15)]);
    compact(&agg).await;
    assert_eq!(id_values(&agg).await, vec![(1, 5)]);

    let builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("ts", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("other", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("record-level.expire-time", "1 s")
        .option("record-level.time-field", "ts")
        .option("num-sorted-run.compaction-trigger", "2");
    let partial = table_with_schema(TableSchema::new(0, &builder.build().unwrap())).await;
    let live = i32::try_from(now_secs() + 3600).unwrap();
    let batch = |ts: Option<i32>, value: Option<i32>, other: Option<i32>| {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("ts", ArrowDataType::Int32, true),
                ArrowField::new("value", ArrowDataType::Int32, true),
                ArrowField::new("other", ArrowDataType::Int32, true),
                ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![ts])),
                Arc::new(Int32Array::from(vec![value])),
                Arc::new(Int32Array::from(vec![other])),
                Arc::new(arrow_array::Int8Array::from(vec![0i8])),
            ],
        )
        .unwrap()
    };
    write_batch(&partial, &batch(Some(1), Some(10), None)).await;
    write_batch(&partial, &batch(Some(live), None, Some(20))).await;
    compact(&partial).await;
    let state = read(&partial, Some(&["value", "other"])).await;
    let batch = &state[0];
    let value = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let other = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    // Expired partial row dropped before merge; only the live sparse update remains.
    assert!(value.is_null(0));
    assert_eq!(other.value(0), 20);
}

#[tokio::test]
async fn expire_rewrite_emits_correct_value_stats() {
    let table = table_with_schema(expire_schema(
        DataType::Int(IntType::new()),
        &[],
        "deduplicate",
    ))
    .await;
    let live = i32::try_from(now_secs() + 3600).unwrap();
    write_batch(
        &table,
        &int_batch(vec![1, 2], vec![Some(1), Some(live)], vec![10, 20]),
    )
    .await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    assert!(!results[0].files().is_empty());
    let ts_field = table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == "ts")
        .unwrap();
    for file in results[0].files() {
        let stats = file
            .value_stats_for_field(table.schema().id(), table.schema().fields(), ts_field)
            .expect("rewrite should publish dense value stats for ts");
        assert_eq!(stats.min_value, Some(paimon::spec::Datum::Int(live)));
        assert_eq!(stats.max_value, Some(paimon::spec::Datum::Int(live)));
    }
}

#[tokio::test]
async fn expire_plus_clustering_still_rejected() {
    let table = table(&[
        ("record-level.expire-time", "1 d"),
        ("record-level.time-field", "value"),
        ("pk-clustering-override", "true"),
        ("clustering.columns", "value"),
    ])
    .await;
    let err = match table.new_compact_builder().new_planner() {
        Ok(_) => panic!("expected expire + clustering to be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(err.contains("pk-clustering-override"));
}

/// Fully expired max-level files must be rewritten with empty output and retired.
///
/// Seed without expire so `ts=1` survives the first compact; then enable
/// `record-level.expire-time=1 s` (deterministic: any wall clock after epoch
/// second 2 treats `ts=1` as expired). Universal picks nothing for a lone
/// max-level run; expire-only rewrite must drop every record, emit no data
/// files, and remove the inputs from the committed Snapshot/Manifest.
#[tokio::test]
async fn expire_only_max_level_rewrite_retires_fully_expired_files() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("ts", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "deduplicate")
        .option("num-sorted-run.compaction-trigger", "2")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;

    // ts=1 is far in the past once expire-time is enabled; keep it live for seeding.
    write_batch(
        &table,
        &int_batch(vec![1, 2], vec![Some(1), Some(1)], vec![10, 20]),
    )
    .await;
    let seed = table.new_compact_builder();
    let seed_plan = seed.new_planner().unwrap().plan().await.unwrap();
    assert!(!seed_plan.tasks().is_empty());
    let seeded = execute(&table, &seed_plan).await;
    seed.new_commit()
        .unwrap()
        .commit_compact(seeded.clone())
        .await
        .unwrap();
    assert!(!seeded[0].files().is_empty());
    let max_level = seeded[0].files()[0].level;
    assert!(
        max_level > 0,
        "seed compact must materialize positive-level history, got level {max_level}"
    );
    assert!(seeded[0].files().iter().all(|file| file.level == max_level));
    let expired_inputs = seeded[0].files().to_vec();
    assert_eq!(id_values(&table).await, vec![(1, 10), (2, 20)]);

    let table = table.copy_with_options(std::collections::HashMap::from([
        ("record-level.expire-time".into(), "1 s".into()),
        ("record-level.time-field".into(), "ts".into()),
    ]));
    let ts_field = table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == "ts")
        .unwrap();
    for file in &expired_inputs {
        let stats = file
            .value_stats_for_field(table.schema().id(), table.schema().fields(), ts_field)
            .expect("seeded max-level file must expose ts stats for expire selection");
        assert_eq!(stats.min_value, Some(paimon::spec::Datum::Int(1)));
        assert_eq!(stats.max_value, Some(paimon::spec::Datum::Int(1)));
    }

    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(
        plan.tasks().len(),
        1,
        "expected a single expire-only rewrite task"
    );
    let task = &plan.tasks()[0];
    assert!(
        task.files().iter().all(|file| file.level == max_level),
        "expire-only rewrite must select only max-level inputs"
    );
    let selected: std::collections::HashSet<_> = task
        .files()
        .iter()
        .map(|file| file.file_name.as_str())
        .collect();
    for file in &expired_inputs {
        assert!(
            selected.contains(file.file_name.as_str()),
            "planner must select seeded expired file {}",
            file.file_name
        );
    }

    let results = execute(&table, &plan).await;
    assert_eq!(results.len(), 1);
    assert!(
        results[0].files().is_empty(),
        "all-expired rewrite must emit no data files"
    );
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();

    let active = active_files(&table).await;
    for file in &expired_inputs {
        assert!(
            active
                .iter()
                .all(|alive| alive.file_name != file.file_name),
            "expired input {} must be retired from the committed snapshot",
            file.file_name
        );
    }
    assert!(
        active.is_empty(),
        "no residual data files expected after retiring fully expired max-level inputs"
    );
    assert!(id_values(&table).await.is_empty());
    assert!(all_id_values(&table).await.is_empty());
}

async fn lookup_compact(table: &Table) -> i64 {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(!plan.tasks().is_empty());
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(table, &plan).await)
        .await
        .unwrap();
    table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap()
}

/// Seed a large max-level history, then enable expire for the next compact.
///
/// Padding keeps the max-level run large enough that force-L0 stops before it,
/// matching Java Lookup incremental compaction (history stays on LevelLookup).
async fn seed_lookup_history_then_enable_expire(ts: i32, value: i32) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("ts", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("changelog-producer", "lookup")
        .option("num-sorted-run.compaction-trigger", "2")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    let mut ids = vec![1];
    let mut timestamps = vec![Some(ts)];
    let mut values = vec![value];
    for id in 2..=1000 {
        ids.push(id);
        timestamps.push(Some(ts));
        values.push(id);
    }
    write_batch(&table, &int_batch(ids, timestamps, values)).await;
    lookup_compact(&table).await;
    assert_eq!(id_value(&table, 1).await, Some(value));
    table.copy_with_options(std::collections::HashMap::from([
        ("record-level.expire-time".into(), "1 s".into()),
        ("record-level.time-field".into(), "ts".into()),
    ]))
}

fn live_ts() -> i32 {
    i32::try_from(now_secs() + 3600).unwrap()
}

#[tokio::test]
async fn lookup_expire_no_changelog_when_history_and_input_all_expired() {
    let table = seed_lookup_history_then_enable_expire(1, 10).await;
    let before = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    // Fully expired L0 never enters Lookup; Java emits no expiration-only -D.
    // Large max-level history is left to LevelLookup and remains readable.
    write_batch(&table, &int_batch(vec![1], vec![Some(0)], vec![99])).await;
    let after = lookup_compact(&table).await;
    assert_eq!(id_value(&table, 1).await, Some(10));
    assert!(super::lookup_changelog_tests::events(&table, before, after)
        .await
        .is_empty());
}

#[tokio::test]
async fn lookup_expire_insert_when_expired_history_and_live_input() {
    let table = seed_lookup_history_then_enable_expire(1, 10).await;
    let before = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    let live = live_ts();
    write_batch(&table, &int_batch(vec![1], vec![Some(live)], vec![11])).await;
    let after = lookup_compact(&table).await;
    assert_eq!(id_value(&table, 1).await, Some(11));
    assert_eq!(
        super::lookup_changelog_tests::events(&table, before, after).await,
        vec![("+I".into(), 1, 11)]
    );
}

#[tokio::test]
async fn lookup_expire_no_changelog_when_live_history_and_input_all_expired() {
    let table = seed_lookup_history_then_enable_expire(live_ts(), 10).await;
    let before = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    write_batch(&table, &int_batch(vec![1], vec![Some(0)], vec![99])).await;
    let after = lookup_compact(&table).await;
    assert_eq!(id_value(&table, 1).await, Some(10));
    assert!(super::lookup_changelog_tests::events(&table, before, after)
        .await
        .is_empty());
}

#[tokio::test]
async fn lookup_expire_update_when_live_history_and_live_input() {
    let table = seed_lookup_history_then_enable_expire(live_ts(), 10).await;
    let before = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    write_batch(
        &table,
        &int_batch(vec![1], vec![Some(live_ts())], vec![11]),
    )
    .await;
    let after = lookup_compact(&table).await;
    assert_eq!(id_value(&table, 1).await, Some(11));
    assert_eq!(
        super::lookup_changelog_tests::events(&table, before, after).await,
        vec![("-U".into(), 1, 10), ("+U".into(), 1, 11)]
    );
}

#[tokio::test]
async fn lookup_expire_no_changelog_when_no_history_and_input_all_expired() {
    let table = table_with_schema(expire_schema(
        DataType::Int(IntType::new()),
        &[("changelog-producer", "lookup")],
        "deduplicate",
    ))
    .await;
    let before = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap_or(0);
    write_batch(&table, &int_batch(vec![1], vec![Some(0)], vec![10])).await;
    write_batch(&table, &int_batch(vec![2], vec![Some(0)], vec![20])).await;
    let after = lookup_compact(&table).await;
    assert!(id_values(&table).await.is_empty());
    assert!(super::lookup_changelog_tests::events(&table, before, after)
        .await
        .is_empty());
}

#[tokio::test]
async fn non_lookup_expire_still_drops_expired_without_changelog() {
    let table = table_with_schema(expire_schema(
        DataType::Int(IntType::new()),
        &[],
        "deduplicate",
    ))
    .await;
    let live = i32::try_from(now_secs() + 3600).unwrap();
    write_batch(
        &table,
        &int_batch(vec![1, 2], vec![Some(1), Some(live)], vec![10, 20]),
    )
    .await;
    assert_eq!(id_values(&table).await, vec![(1, 10), (2, 20)]);
    compact(&table).await;
    assert_eq!(id_values(&table).await, vec![(2, 20)]);
}
