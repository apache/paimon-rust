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

async fn lookup_table(options: &[(&str, &str)]) -> Table {
    let mut configured = vec![("changelog-producer", "lookup")];
    configured.extend_from_slice(options);
    table(&configured).await
}

async fn compact(table: &Table) -> i64 {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
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

pub(super) async fn events(table: &Table, start: i64, end: i64) -> Vec<(String, i32, i32)> {
    let plan = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Auto, start, end)
        .plan_combined()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut events = Vec::new();
    for batch in batches {
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
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
        events.extend((0..batch.num_rows()).map(|row| {
            (
                kinds.value(row).to_string(),
                ids.value(row),
                values.value(row),
            )
        }));
    }
    events.sort();
    events
}

fn expected(events: &[(&str, i32, i32)]) -> Vec<(String, i32, i32)> {
    let mut events = events
        .iter()
        .map(|(kind, key, value)| (kind.to_string(), *key, *value))
        .collect::<Vec<_>>();
    events.sort();
    events
}

#[tokio::test]
async fn lookup_compaction_publishes_before_after_images_for_fixed_and_dynamic_tables() {
    for dv in ["false"] {
        for bucket in ["1", "-1"] {
            let table = lookup_table(&[
                ("deletion-vectors.enabled", dv),
                ("bucket", bucket),
                ("read.batch-size", "1"),
            ])
            .await;
            write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
            assert!(events(&table, 0, 1).await.is_empty());
            let first = compact(&table).await;
            assert_eq!(
                events(&table, 0, first).await,
                expected(&[("+I", 1, 10), ("+I", 2, 20)])
            );
            write_batch(
                &table,
                &make_batch_with_kinds(vec![1, 2, 3], vec![11, 999, 30], vec![0, 3, 0]),
            )
            .await;
            let second = compact(&table).await;
            assert_eq!(
                events(&table, first, second).await,
                expected(&[("-U", 1, 10), ("+U", 1, 11), ("-D", 2, 20), ("+I", 3, 30)])
            );
            assert_eq!(rows(&table).await, vec![(1, 11), (3, 30)]);
            assert_eq!(
                compact(&table).await,
                second,
                "an already compacted bucket emits nothing"
            );
            assert_eq!(events(&table, first, second).await.len(), 4);
        }
    }
}

#[tokio::test]
async fn lookup_compaction_respects_java_changelog_flags_and_default_duplicate_updates() {
    for (options, expected_events) in [
        (
            vec![],
            expected(&[("-U", 1, 10), ("+U", 1, 10), ("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.row-deduplicate", "true")],
            expected(&[("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.ignore-update-before", "true")],
            expected(&[("+U", 1, 10), ("-D", 2, 20)]),
        ),
        (
            vec![("changelog-producer.ignore-delete", "true")],
            expected(&[("-U", 1, 10), ("+U", 1, 10)]),
        ),
        (
            vec![
                ("changelog-producer.row-deduplicate", "true"),
                ("changelog-producer.row-deduplicate-ignore-fields", "value"),
                ("changelog-producer.ignore-delete", "true"),
            ],
            vec![],
        ),
    ] {
        let table = lookup_table(&options).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let first = compact(&table).await;
        let updated_value = if options
            .iter()
            .any(|(key, _)| *key == "changelog-producer.row-deduplicate-ignore-fields")
        {
            11
        } else {
            10
        };
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1, 2], vec![updated_value, 0], vec![0, 3]),
        )
        .await;
        let second = compact(&table).await;
        assert_eq!(
            events(&table, first, second).await,
            expected_events,
            "{options:?}"
        );
        assert_eq!(rows(&table).await, vec![(1, updated_value)]);
    }
}

#[tokio::test]
async fn lookup_row_deduplication_compares_map_entries_by_key() {
    use arrow_array::builder::{Int32Builder, MapBuilder};
    use paimon::spec::{DataField, MapType, RowType};
    for nested in [false, true] {
        let map_type = DataType::Map(MapType::new(
            DataType::Int(IntType::new()),
            DataType::Int(IntType::new()),
        ));
        let value_type = if nested {
            DataType::Row(RowType::new(vec![DataField::new(
                2,
                "m".into(),
                map_type.clone(),
            )]))
        } else {
            map_type
        };
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", value_type)
            .primary_key(["id"])
            .option("bucket", "1")
            .option("changelog-producer", "lookup")
            .option("changelog-producer.row-deduplicate", "true")
            .option("changelog-producer.row-deduplicate-ignore-fields", "id")
            .build()
            .unwrap();
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        let batch = |entries: &[(i32, Option<i32>)]| {
            let mut maps = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
            for (key, value) in entries {
                maps.keys().append_value(*key);
                maps.values().append_option(*value);
            }
            maps.append(true).unwrap();
            let value = std::sync::Arc::new(maps.finish()) as arrow_array::ArrayRef;
            let value = if nested {
                std::sync::Arc::new(arrow_array::StructArray::from(vec![(
                    std::sync::Arc::new(arrow_schema::Field::new(
                        "m",
                        value.data_type().clone(),
                        true,
                    )),
                    value,
                )])) as arrow_array::ArrayRef
            } else {
                value
            };
            RecordBatch::try_from_iter([
                (
                    "id",
                    std::sync::Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
                ),
                ("value", value),
            ])
            .unwrap()
        };
        let payloads = |batch: &RecordBatch| {
            let value = batch.column_by_name("value").unwrap();
            let value = if nested {
                value
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .unwrap()
                    .column(0)
            } else {
                value
            };
            let maps = value
                .as_any()
                .downcast_ref::<arrow_array::MapArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    let entries = maps.value(row);
                    let keys = entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let values = entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    (0..entries.len())
                        .map(|index| {
                            (
                                keys.value(index),
                                (!values.is_null(index)).then(|| values.value(index)),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let mut previous = vec![(1, Some(10)), (2, None)];
        write_batch(&table, &batch(&previous)).await;
        let mut start = compact(&table).await;
        for (entries, count) in [
            (vec![(2, None), (1, Some(10))], 0),
            (vec![(2, Some(20)), (1, Some(10))], 2),
            (vec![(1, Some(10)), (2, Some(20))], 0),
            (vec![(1, Some(10)), (3, Some(20))], 2),
            (vec![], 2),
            (vec![], 0),
        ] {
            write_batch(&table, &batch(&entries)).await;
            let end = compact(&table).await;
            let plan = table
                .new_read_builder()
                .new_incremental_scan(IncrementalScanMode::Changelog, start, end)
                .plan_combined()
                .await
                .unwrap();
            let logs: Vec<RecordBatch> = table
                .new_read_builder()
                .new_read()
                .unwrap()
                .to_arrow_with_row_kind(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(logs.iter().map(RecordBatch::num_rows).sum::<usize>(), count);
            if count != 0 {
                let kinds = logs
                    .iter()
                    .flat_map(|batch| {
                        let kinds = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        (0..batch.num_rows())
                            .map(|row| kinds.value(row).to_string())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(kinds, vec!["-U", "+U"]);
                assert_eq!(
                    logs.iter().flat_map(&payloads).collect::<Vec<_>>(),
                    vec![previous.clone(), entries.clone()]
                );
            }
            let data = read(&table, Some(&["value"])).await;
            assert_eq!(
                data.iter().flat_map(&payloads).collect::<Vec<_>>(),
                vec![entries.clone()]
            );
            previous = entries;
            start = end;
        }
    }
}

#[tokio::test]
async fn lookup_compaction_first_row_only_emits_new_keys() {
    let table = lookup_table(&[("merge-engine", "first-row")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let first = compact(&table).await;
    assert_eq!(events(&table, 0, first).await, expected(&[("+I", 1, 10)]));
    write_batch(&table, &make_batch(vec![1, 2], vec![99, 20])).await;
    let second = compact(&table).await;
    assert_eq!(
        events(&table, first, second).await,
        expected(&[("+I", 2, 20)])
    );
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn lookup_first_row_preserves_the_earliest_record_across_positive_levels() {
    for with_l0 in [false, true] {
        let table = lookup_table(&[
            ("merge-engine", "first-row"),
            ("num-sorted-run.compaction-trigger", "2"),
            ("num-levels", "6"),
        ])
        .await;
        append_at_level(&table, 10, 3).await;
        append_at_level(&table, 20, 2).await;
        let history = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        if with_l0 {
            write_batch(&table, &make_batch(vec![1], vec![30])).await;
        }
        let end = compact(&table).await;
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .commit_kind(),
            &CommitKind::COMPACT
        );
        assert_eq!(rows(&table).await, vec![(1, 10)]);
        assert!(events(&table, history, end).await.is_empty());
    }
}

#[tokio::test]
async fn lookup_first_row_preserves_the_first_add_kind() {
    let table = lookup_table(&[("merge-engine", "first-row")]).await;
    write_batch(&table, &make_batch_with_kinds(vec![1], vec![10], vec![2])).await;
    let end = compact(&table).await;
    assert_eq!(events(&table, 0, end).await, expected(&[("+U", 1, 10)]));
    assert_eq!(rows(&table).await, vec![(1, 10)]);
}

#[tokio::test]
async fn lookup_first_row_ignores_retract_history_before_deciding_whether_a_key_is_new() {
    for existing in [false, true] {
        let table = lookup_table(&[("merge-engine", "first-row"), ("ignore-delete", "true")]).await;
        if existing {
            append_at_level(&table, 10, 3).await;
        }
        // Older Java first-row files can retain retracts even when deletes are
        // ignored. A deduplicate writer creates that legacy physical input.
        let legacy = table.copy_with_options(std::collections::HashMap::from([
            ("merge-engine".into(), "deduplicate".into()),
            ("ignore-delete".into(), "false".into()),
        ]));
        append_kind_at_level(&legacy, 20, 2, 3).await;
        let history = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        write_batch(&table, &make_batch(vec![1], vec![30])).await;
        let end = compact(&table).await;
        assert_eq!(
            rows(&table).await,
            vec![(1, if existing { 10 } else { 30 })]
        );
        assert_eq!(
            events(&table, history, end).await,
            if existing {
                vec![]
            } else {
                expected(&[("+I", 1, 30)])
            }
        );
    }
}

#[tokio::test]
async fn lookup_aggregation_reduces_singleton_retracts_like_java() {
    for remove in ["false", "true"] {
        for kind in [1, 3] {
            let table = lookup_table(&[
                ("merge-engine", "aggregation"),
                ("fields.value.aggregate-function", "sum"),
                ("aggregation.remove-record-on-delete", remove),
            ])
            .await;
            write_batch(&table, &make_batch_with_kinds(vec![1], vec![5], vec![kind])).await;
            let deleted = remove == "true" && kind == 3;
            let values = if deleted { vec![] } else { vec![(1, -5)] };
            // Java query wraps the lookup function in a singleton reducer;
            // lookup compact instead runs the raw aggregator.
            assert!(
                rows(&table).await.is_empty(),
                "query: remove={remove}, kind={kind}"
            );
            let end = compact(&table).await;
            assert_eq!(
                rows(&table).await,
                values,
                "compact: remove={remove}, kind={kind}"
            );
            assert_eq!(
                events(&table, 0, end).await,
                if deleted {
                    vec![]
                } else {
                    expected(&[("+I", 1, -5)])
                }
            );
        }
    }
}

#[tokio::test]
async fn lookup_partial_update_preserves_a_single_sequence_group_retract() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("seq", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("changelog-producer", "lookup")
        .option("fields.seq.sequence-group", "value")
        .option("fields.value.aggregate-function", "sum")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    let batch = |seq, value, kind| {
        RecordBatch::try_from_iter([
            (
                "id",
                std::sync::Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            ),
            (
                "seq",
                std::sync::Arc::new(Int32Array::from(vec![seq])) as arrow_array::ArrayRef,
            ),
            (
                "value",
                std::sync::Arc::new(Int32Array::from(vec![value])) as arrow_array::ArrayRef,
            ),
            (
                "_VALUE_KIND",
                std::sync::Arc::new(arrow_array::Int8Array::from(vec![kind]))
                    as arrow_array::ArrayRef,
            ),
        ])
        .unwrap()
    };
    write_batch(&table, &batch(10, 5, 3)).await;
    let deleted = compact(&table).await;
    assert!(read(&table, Some(&["value"])).await.is_empty());
    assert!(events(&table, 0, deleted).await.is_empty());
    write_batch(&table, &batch(11, 7, 0)).await;
    let end = compact(&table).await;
    assert_eq!(
        events(&table, deleted, end).await,
        expected(&[("+I", 1, 7)])
    );
    let actual = read(&table, Some(&["value"])).await;
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        actual[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
}

#[tokio::test]
async fn lookup_partial_update_retains_ignored_legacy_delete_payload() {
    for aggregate in [None, Some("sum"), Some("first_value")] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("b", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "partial-update")
            .option("changelog-producer", "lookup")
            .option("ignore-delete", "true");
        if let Some(aggregate) = aggregate {
            schema = schema
                .option("fields.b.sequence-group", "value")
                .option("fields.value.aggregate-function", aggregate);
        }
        let schema = schema.build().unwrap();
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        let batch = |value, b, kind| {
            RecordBatch::try_from_iter([
                (
                    "id",
                    std::sync::Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    std::sync::Arc::new(Int32Array::from(vec![value])) as arrow_array::ArrayRef,
                ),
                (
                    "b",
                    std::sync::Arc::new(Int32Array::from(vec![Some(b)])) as arrow_array::ArrayRef,
                ),
                (
                    "_VALUE_KIND",
                    std::sync::Arc::new(arrow_array::Int8Array::from(vec![kind]))
                        as arrow_array::ArrayRef,
                ),
            ])
            .unwrap()
        };
        let legacy = table.copy_with_options(std::collections::HashMap::from([
            ("merge-engine".into(), "deduplicate".into()),
            ("ignore-delete".into(), "false".into()),
        ]));
        write_batch(&legacy, &batch(Some(10), 20, 3)).await;
        write_batch(&legacy, &batch(Some(99), 21, 3)).await;
        let deleted = compact(&table).await;
        assert!(events(&table, 0, deleted).await.is_empty());
        let scan = table.new_read_builder().new_scan().plan().await.unwrap();
        let files = scan
            .splits()
            .iter()
            .flat_map(|split| split.data_files())
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].delete_row_count, Some(1));
        assert_eq!(
            (files[0].min_sequence_number, files[0].max_sequence_number),
            (0, 0)
        );
        write_batch(&table, &batch(aggregate.map(|_| 7), 30, 0)).await;
        let end = compact(&table).await;
        let actual = read(&table, Some(&["value", "b"])).await;
        assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let value = actual[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(
            !value.is_null(0),
            "ignored delete must seed the unchanged column"
        );
        let expected_value = match aggregate {
            None => 10,
            Some("sum") => 17,
            Some("first_value") => 7,
            _ => unreachable!(),
        };
        assert_eq!(value.value(0), expected_value, "aggregate={aggregate:?}");
        assert_eq!(
            actual[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            30
        );
        assert_eq!(
            events(&table, deleted, end).await,
            expected(&[("+I", 1, expected_value)])
        );
    }
}

#[tokio::test]
async fn lookup_compaction_materializes_aggregation_history_once_and_ignores_high_level_only_keys()
{
    for dv in ["false"] {
        let table = lookup_table(&[
            ("deletion-vectors.enabled", dv),
            ("merge-engine", "aggregation"),
            ("fields.value.aggregate-function", "sum"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let first = compact(&table).await;
        write_batch(&table, &make_batch(vec![1, 3], vec![5, 30])).await;
        let second = compact(&table).await;
        assert_eq!(
            events(&table, first, second).await,
            expected(&[("-U", 1, 10), ("+U", 1, 15), ("+I", 3, 30)])
        );
        write_batch(&table, &make_batch(vec![1], vec![7])).await;
        let third = compact(&table).await;
        assert_eq!(
            events(&table, second, third).await,
            expected(&[("-U", 1, 15), ("+U", 1, 22)])
        );
        assert_eq!(rows(&table).await, vec![(1, 22), (2, 20), (3, 30)]);
    }
}

async fn append_at_level(table: &Table, value: i32, level: i32) {
    append_kind_at_level(table, value, level, 0).await;
}

async fn append_kind_at_level(table: &Table, value: i32, level: i32, kind: i8) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch_with_kinds(vec![1], vec![value], vec![kind]))
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    for file in messages
        .iter_mut()
        .flat_map(|message| &mut message.new_files)
    {
        file.level = level;
    }
    builder.new_commit().commit(messages).await.unwrap();
    writer.close().await;
}

#[tokio::test]
async fn lookup_compaction_subsets_and_retries_publish_each_changelog_once_with_concurrent_l0() {
    for dv in ["false"] {
        let table = lookup_table(&[
            ("bucket", "-1"),
            ("dynamic-bucket.target-row-num", "1"),
            ("deletion-vectors.enabled", dv),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let first = compact(&table).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![11, 21])).await;
        let builder = table
            .new_compact_builder()
            .with_commit_user("lookup-retry")
            .unwrap();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks().len(), 2);
        let results = execute(&table, &plan).await;
        assert_eq!(
            results
                .iter()
                .map(|result| result.metrics().changelog_rows_written)
                .sum::<u64>(),
            4
        );
        write_batch(&table, &make_batch(vec![1], vec![12])).await;
        let commit = builder.new_commit().unwrap();
        for result in results {
            let metrics = commit.commit_compact(vec![result.clone()]).await.unwrap();
            let snapshot = table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap();
            assert_eq!(commit.commit_compact(vec![result]).await.unwrap(), metrics);
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                snapshot
            );
        }
        let second = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            events(&table, first, second).await,
            expected(&[("-U", 1, 10), ("+U", 1, 11), ("-U", 2, 20), ("+U", 2, 21)])
        );
        let third = compact(&table).await;
        assert_eq!(
            events(&table, second, third).await,
            expected(&[("-U", 1, 11), ("+U", 1, 12)])
        );
        assert_eq!(rows(&table).await, vec![(1, 12), (2, 21)]);
    }
}

#[tokio::test]
async fn lookup_compaction_allows_disjoint_history_and_rejects_output_level_overlap() {
    for overlaps in [false, true] {
        let table = lookup_table(&[]).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        compact(&table).await;
        write_batch(&table, &make_batch(vec![1], vec![11])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        let output_level = results[0].files()[0].level;
        assert!(output_level > 1);
        let write = table.new_write_builder();
        let mut writer = write.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![if overlaps { 1 } else { 256 }], vec![12]))
            .await
            .unwrap();
        let mut messages = writer.prepare_commit().await.unwrap();
        for file in messages
            .iter_mut()
            .flat_map(|message| &mut message.new_files)
        {
            file.level = if overlaps {
                output_level
            } else {
                output_level - 1
            };
        }
        write.new_commit().commit(messages).await.unwrap();
        writer.close().await;
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap();
        let result = builder.new_commit().unwrap().commit_compact(results).await;
        if overlaps {
            assert!(result.unwrap_err().to_string().contains("LSM"));
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                snapshot
            );
        } else {
            result.unwrap();
            assert_eq!(rows(&table).await, vec![(1, 11), (256, 12)]);
        }
    }
}

#[tokio::test]
async fn lookup_compaction_aborts_data_and_changelog_files_on_a_late_read_failure() {
    let table = lookup_table(&[
        ("read.batch-size", "1"),
        ("write-buffer-size", "1b"),
        ("target-file-row-num", "1"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let scan = table.new_read_builder().new_scan().plan().await.unwrap();
    let second = plan.tasks()[0]
        .files()
        .iter()
        .find(|file| file.max_sequence_number == 1)
        .unwrap();
    table
        .file_io()
        .delete_file(&format!(
            "{}/{}",
            scan.splits()[0].bucket_path(),
            second.file_name
        ))
        .await
        .unwrap();
    let paths = || async {
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .into_iter()
            .map(|file| file.path)
            .collect::<std::collections::BTreeSet<_>>()
    };
    let before = paths().await;
    assert!(builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .is_err());
    assert_eq!(paths().await, before);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn lookup_compaction_validates_changelog_flags_before_execution() {
    for key in [
        "changelog-producer.row-deduplicate",
        "changelog-producer.ignore-update-before",
        "changelog-producer.ignore-delete",
    ] {
        let table = lookup_table(&[(key, "invalid")]).await;
        assert!(table
            .new_compact_builder()
            .new_planner()
            .err()
            .unwrap()
            .to_string()
            .contains(key));
    }
}

#[tokio::test]
async fn lookup_compaction_requires_the_catalog_cache() {
    for (engine, producer) in [
        ("deduplicate", "lookup"),
        ("first-row", "lookup"),
        ("first-row", "none"),
        ("partial-update", "lookup"),
        ("aggregation", "lookup"),
    ] {
        let table = table_with_cache(
            pk_schema(&[
                ("merge-engine", engine),
                ("changelog-producer", producer),
                ("local-cache.enabled", "true"),
            ]),
            &[("local-cache.enabled", "false")],
        )
        .await;
        assert!(table
            .new_compact_builder()
            .new_executor()
            .err()
            .unwrap()
            .to_string()
            .contains("local-cache.enabled=true"));
    }
}

#[tokio::test]
async fn lookup_compaction_selects_the_lowest_positive_level_as_materialized_history() {
    for with_l0 in [false, true] {
        let table = lookup_table(&[
            ("merge-engine", "aggregation"),
            ("fields.value.aggregate-function", "sum"),
            ("num-sorted-run.compaction-trigger", "2"),
            ("num-levels", "6"),
        ])
        .await;
        // L2 already includes L3. Reducing both would count the history twice.
        append_at_level(&table, 10, 3).await;
        append_at_level(&table, 15, 2).await;
        let history = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        if with_l0 {
            write_batch(&table, &make_batch(vec![1], vec![5])).await;
        }
        let expected_value = if with_l0 { 20 } else { 15 };
        assert_eq!(rows(&table).await, vec![(1, expected_value)]);
        let end = compact(&table).await;
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .commit_kind(),
            &CommitKind::COMPACT
        );
        assert_eq!(
            events(&table, history, end).await,
            if with_l0 {
                expected(&[("-U", 1, 15), ("+U", 1, 20)])
            } else {
                vec![]
            }
        );
        assert_eq!(rows(&table).await, vec![(1, expected_value)]);
    }
}
