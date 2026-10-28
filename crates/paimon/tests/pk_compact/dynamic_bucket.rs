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
use paimon::spec::{IndexManifest, IndexManifestEntry, Snapshot};
use std::collections::BTreeMap;

async fn latest(table: &Table) -> Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn indexes(table: &Table) -> Vec<IndexManifestEntry> {
    let snapshot = latest(table).await;
    let Some(name) = snapshot.index_manifest() else {
        return vec![];
    };
    IndexManifest::read(
        table.file_io(),
        &table.snapshot_manager().manifest_path(name),
    )
    .await
    .unwrap()
}

async fn compact(table: &Table) {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(!plan.tasks().is_empty());
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(table, &plan).await)
        .await
        .unwrap();
}

// Read each bucket separately so a lost HASH mapping cannot hide behind an
// otherwise correct full-table result before the next writer restarts.
async fn bucket_rows(table: &Table) -> BTreeMap<i32, (i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan_all().await.unwrap();
    let read = builder.new_read().unwrap();
    let mut result = BTreeMap::new();
    for split in plan.splits() {
        assert_eq!(split.total_buckets(), -1);
        let batches: Vec<RecordBatch> = read
            .to_arrow(std::slice::from_ref(split))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
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
            for i in 0..batch.num_rows() {
                assert!(result
                    .insert(ids.value(i), (split.bucket(), values.value(i)))
                    .is_none());
            }
        }
    }
    result
}

#[tokio::test]
async fn dynamic_compaction_preserves_hash_assignments_for_all_merge_engines() {
    for engine in ["deduplicate", "first-row", "partial-update", "aggregation"] {
        for in_data_dir in ["false", "true"] {
            let mut options = vec![
                ("bucket", "-1"),
                ("dynamic-bucket.target-row-num", "1"),
                ("merge-engine", engine),
                ("index-file-in-data-file-dir", in_data_dir),
            ];
            if engine == "aggregation" {
                options.push(("fields.value.aggregate-function", "sum"));
            }
            let table = table(&options).await;
            write_batch(&table, &make_batch(vec![1, 2, 3], vec![10, 20, 30])).await;
            write_batch(&table, &make_batch(vec![1, 2], vec![11, 21])).await;
            let before = bucket_rows(&table).await;
            assert!(before.values().map(|r| r.0).max().unwrap() > 0);
            let snapshot = latest(&table).await;
            let original_indexes = indexes(&table).await;
            assert_eq!(original_indexes.len(), 3);
            for entry in &original_indexes {
                let dir = if in_data_dir == "true" {
                    format!("bucket-{}", entry.bucket)
                } else {
                    "index".into()
                };
                let path = format!("{}/{dir}/{}", table.location(), entry.index_file.file_name);
                assert!(table.file_io().exists(&path).await.unwrap(), "{path}");
            }
            compact(&table).await;
            assert_eq!(bucket_rows(&table).await, before, "{engine}");
            assert_eq!(
                latest(&table).await.index_manifest(),
                snapshot.index_manifest()
            );
            assert_eq!(indexes(&table).await, original_indexes);

            // A fresh writer restores HASH assignments and capacity after compaction.
            write_batch(&table, &make_batch(vec![1, 4], vec![100, 40])).await;
            compact(&table).await;
            let after = bucket_rows(&table).await;
            assert_eq!(after.len(), 4);
            let value = match engine {
                "first-row" => 10,
                "aggregation" => 121,
                _ => 100,
            };
            assert_eq!(after[&1], (before[&1].0, value));
            assert_eq!(after[&2], before[&2]);
            assert_eq!(after[&3], before[&3]);
            assert_eq!(after[&4].1, 40);
            assert!(after[&4].0 > before.values().map(|r| r.0).max().unwrap());
        }
    }
}

#[tokio::test]
async fn dynamic_compaction_commits_the_latest_hash_index_after_concurrent_append() {
    let table = table(&[
        ("bucket", "-1"),
        ("dynamic-bucket.target-row-num", "2"),
        ("dynamic-bucket.max-buckets", "1"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![11])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let captured = latest(&table).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![12, 20])).await;
    let appended = latest(&table).await;
    let appended_indexes = indexes(&table).await;
    assert_ne!(captured.index_manifest(), appended.index_manifest());
    let commit = builder.new_commit().unwrap();
    commit.commit_compact(results.clone()).await.unwrap();
    assert_eq!(
        latest(&table).await.index_manifest(),
        appended.index_manifest()
    );
    assert_eq!(indexes(&table).await, appended_indexes);
    assert_eq!(rows(&table).await, vec![(1, 12), (2, 20)]);
    let snapshot_id = latest(&table).await.id();
    commit.commit_compact(results).await.unwrap();
    assert_eq!(latest(&table).await.id(), snapshot_id);
    write_batch(&table, &make_batch(vec![2, 3], vec![21, 30])).await;
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 12), (2, 21), (3, 30)]);
}

#[tokio::test]
async fn dynamic_compaction_retains_deleted_hashes_and_bucket_capacity() {
    let table = table(&[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let before = bucket_rows(&table).await;
    write_batch(&table, &make_batch_with_kinds(vec![1], vec![10], vec![3])).await;
    let original_indexes = indexes(&table).await;
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(2, 20)]);
    assert_eq!(indexes(&table).await, original_indexes);
    write_batch(&table, &make_batch(vec![1, 3], vec![11, 30])).await;
    let after = bucket_rows(&table).await;
    assert_eq!(after[&1], (before[&1].0, 11));
    assert_eq!(after[&2], before[&2]);
    assert!(after[&3].0 > before.values().map(|r| r.0).max().unwrap());
}

async fn partitioned(cross_partition: bool, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(if cross_partition {
            vec!["id"]
        } else {
            vec!["pt", "id"]
        })
        .partition_keys(["pt"])
        .option("bucket", "-1")
        .option("dynamic-bucket.target-row-num", "1");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    table_with_schema(TableSchema::new(0, &schema.build().unwrap())).await
}

async fn partition_rows(table: &Table) -> Vec<(String, i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan_all().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = vec![];
    for batch in batches {
        let pts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        result.extend(
            (0..batch.num_rows())
                .map(|i| (pts.value(i).to_string(), ids.value(i), values.value(i))),
        );
    }
    result.sort();
    result
}

#[tokio::test]
async fn dynamic_partition_compaction_keeps_unselected_partitions_and_indexes() {
    let table = partitioned(false, &[]).await;
    for values in [vec![10, 20], vec![11, 21]] {
        write_batch(
            &table,
            &make_partitioned_batch(vec!["a", "b"], vec![1, 2], values),
        )
        .await;
    }
    let before = indexes(&table).await;
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("pt", Datum::String("a".into()))
        .unwrap();
    let builder = table
        .new_compact_builder()
        .with_partition_filter(predicate)
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(&table, &plan).await)
        .await
        .unwrap();
    assert_eq!(indexes(&table).await, before);
    assert_eq!(
        partition_rows(&table).await,
        vec![("a".into(), 1, 11), ("b".into(), 2, 21)]
    );
    let plan = table
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(plan.tasks().len(), 1, "only partition b still has L0 files");
    write_batch(
        &table,
        &make_partitioned_batch(vec!["a", "b"], vec![1, 2], vec![12, 22]),
    )
    .await;
    compact(&table).await;
    assert_eq!(
        partition_rows(&table).await,
        vec![("a".into(), 1, 12), ("b".into(), 2, 22)]
    );
}

#[tokio::test]
async fn cross_partition_compaction_preserves_migrations_and_restart_for_all_engines() {
    for engine in ["deduplicate", "first-row", "partial-update", "aggregation"] {
        let mut options = vec![("merge-engine", engine)];
        if engine == "aggregation" {
            options.push(("fields.value.aggregate-function", "sum"));
        }
        let table = partitioned(true, &options).await;
        write_batch(
            &table,
            &make_partitioned_batch(vec!["a"], vec![1], vec![10]),
        )
        .await;
        write_batch(
            &table,
            &make_partitioned_batch(vec!["b"], vec![1], vec![20]),
        )
        .await;
        let before = partition_rows(&table).await;
        compact(&table).await;
        assert_eq!(partition_rows(&table).await, before, "{engine}");
        assert!(latest(&table).await.index_manifest().is_none());
        write_batch(
            &table,
            &make_partitioned_batch(vec!["c"], vec![1], vec![30]),
        )
        .await;
        if engine == "first-row" {
            // The restored global index ignores this existing key altogether.
            assert!(table
                .new_compact_builder()
                .new_planner()
                .unwrap()
                .plan()
                .await
                .unwrap()
                .tasks()
                .is_empty());
        } else {
            compact(&table).await;
        }
        let (partition, value) = match engine {
            "deduplicate" => ("c", 30),
            "first-row" => ("a", 10),
            "partial-update" => ("a", 30),
            "aggregation" => ("a", 60),
            _ => unreachable!(),
        };
        assert_eq!(
            partition_rows(&table).await,
            vec![(partition.into(), 1, value)],
            "{engine}"
        );
    }
}

#[tokio::test]
async fn cross_partition_compaction_preserves_concurrent_migration_deletes() {
    let table = partitioned(true, &[]).await;
    write_batch(
        &table,
        &make_partitioned_batch(vec!["a"], vec![1], vec![10]),
    )
    .await;
    write_batch(
        &table,
        &make_partitioned_batch(vec!["b"], vec![1], vec![20]),
    )
    .await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    write_batch(
        &table,
        &make_partitioned_batch(vec!["c"], vec![1], vec![30]),
    )
    .await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(partition_rows(&table).await, vec![("c".into(), 1, 30)]);
    write_batch(
        &table,
        &make_partitioned_batch(vec!["a"], vec![1], vec![40]),
    )
    .await;
    compact(&table).await;
    assert_eq!(partition_rows(&table).await, vec![("a".into(), 1, 40)]);
}

#[tokio::test]
async fn dynamic_compaction_rejects_other_index_types_at_plan_and_commit() {
    for index_type in [
        "DELETION_VECTORS",
        "GLOBAL_INDEX",
        "UNKNOWN",
        "HASH_WITH_DV_METADATA",
        "HASH_WITH_GLOBAL_METADATA",
    ] {
        let table = table(&[("bucket", "-1")]).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        write_batch(&table, &make_batch(vec![2], vec![20])).await;
        let snapshot = latest(&table).await;
        let mut entries = indexes(&table).await;
        let mut unsupported = entries[0].clone();
        match index_type {
            "HASH_WITH_DV_METADATA" => {
                unsupported.index_file.deletion_vectors_ranges = Some(Default::default());
            }
            "HASH_WITH_GLOBAL_METADATA" => {
                unsupported.index_file.global_index_meta = Some(paimon::spec::GlobalIndexMeta {
                    row_range_start: 0,
                    row_range_end: 1,
                    index_field_id: 0,
                    extra_field_ids: None,
                    index_meta: None,
                    source_meta: None,
                });
            }
            _ => unsupported.index_file.index_type = index_type.into(),
        }
        entries.push(unsupported);
        let manager = table.snapshot_manager();
        let name = "unsupported-index-manifest";
        IndexManifest::write(table.file_io(), &manager.manifest_path(name), &entries)
            .await
            .unwrap();
        let mut altered = serde_json::to_value(&snapshot).unwrap();
        altered["indexManifest"] = name.into();
        table
            .file_io()
            .new_output(&manager.snapshot_path(snapshot.id()))
            .unwrap()
            .write(serde_json::to_vec(&altered).unwrap().into())
            .await
            .unwrap();
        let error = builder.new_planner().unwrap().plan().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("only supports dynamic-bucket HASH"),
            "{error}"
        );
        let error = builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("only supports dynamic-bucket HASH"),
            "{error}"
        );
        assert_eq!(latest(&table).await.id(), snapshot.id());
    }
}
