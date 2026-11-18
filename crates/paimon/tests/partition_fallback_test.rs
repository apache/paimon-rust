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

#![cfg(feature = "storage-fs")]

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::catalog::{Catalog, Identifier};
use paimon::common::Options;
use paimon::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, SchemaChange};
use paimon::table::{BranchManager, ReadBuilder};
use paimon::{CatalogOptions, DataSplit, FileSystemCatalog, Table};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup() -> (tempfile::TempDir, FileSystemCatalog, Identifier, Table) {
    let temp = tempfile::tempdir().unwrap();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, temp.path().to_str().unwrap());
    let catalog = FileSystemCatalog::new(options).unwrap();
    catalog
        .create_database("db", false, HashMap::new())
        .await
        .unwrap();
    let id = Identifier::new("db", "t");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("scan.fallback-branch", "backup")
        .build()
        .unwrap();
    catalog.create_table(&id, schema, false).await.unwrap();
    let table = catalog.get_table(&id).await.unwrap();
    (temp, catalog, id, table)
}

async fn create_branch(table: &Table, name: &str) -> Table {
    BranchManager::new(table.file_io().clone(), table.location().into())
        .create_branch(name)
        .await
        .unwrap();
    table.copy_with_branch(name).await.unwrap()
}

async fn write(table: &Table, ids: Vec<i32>, partitions: impl Into<Int32Array>) {
    let write = table.new_write_builder();
    let mut writer = write.new_write().unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            table.schema().fields()[0].name().to_string(),
            Arc::new(Int32Array::from(ids)) as ArrayRef,
        ),
        ("pt".to_string(), Arc::new(partitions.into()) as ArrayRef),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    write
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn ids(builder: &ReadBuilder<'_>) -> Vec<i32> {
    let plan = builder.new_scan().plan().await.unwrap();
    let transported = plan
        .splits()
        .iter()
        .map(|s| {
            let bytes = s.serialize_split_v1().unwrap();
            let restored = DataSplit::deserialize_split_v1(&bytes).unwrap();
            assert_eq!(s, &restored);
            restored
        })
        .collect::<Vec<_>>();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .with_parallelism(2)
        .unwrap()
        .to_arrow(&transported)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn partition_fallback_is_opt_in_and_owns_partitions_before_filtering() {
    let (_temp, _, _, main) = setup().await;
    let branches = BranchManager::new(main.file_io().clone(), main.location().to_owned());
    branches.create_branch("backup").await.unwrap();
    let backup = main.copy_with_branch("backup").await.unwrap();
    write(&backup, vec![99, 2], vec![10, 20]).await;
    write(&main, vec![1], vec![10]).await;

    let mut read = main.new_read_builder();
    assert_eq!(ids(&read).await, [1]);
    read.with_partition_fallback()
        .unwrap()
        .with_projection(&["id"])
        .unwrap();
    assert_eq!(ids(&read).await, [1, 2]);

    let plan = read.new_scan().plan().await.unwrap();
    assert!(plan.splits().iter().any(DataSplit::is_fallback));
    assert!(main
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .is_err());
    assert!(read
        .new_scan()
        .plan_all()
        .await
        .unwrap()
        .splits()
        .iter()
        .all(|s| !s.is_fallback()));
    for (id, expected) in [(99, vec![]), (2, vec![2])] {
        let mut filtered = read.clone();
        filtered.with_filter(
            PredicateBuilder::new(main.schema().fields())
                .equal("id", Datum::Int(id))
                .unwrap(),
        );
        assert_eq!(ids(&filtered).await, expected);
    }
    let mut pruned = read.clone();
    let only_backup = PredicateBuilder::new(main.schema().fields())
        .equal("pt", Datum::Int(20))
        .unwrap();
    pruned.with_partition_filter(Some(only_backup)).unwrap();
    assert_eq!(ids(&pruned).await, [2]);
    assert_eq!(ids(read.with_limit(1)).await.len(), 1);
    assert!(ids(read.with_limit(0)).await.is_empty());
}

#[tokio::test]
async fn partition_fallback_handles_empty_and_missing_branches() {
    let (_temp, _, _, main) = setup().await;
    let mut read = main.new_read_builder();
    read.with_partition_fallback().unwrap();
    assert!(ids(&read).await.is_empty());
    let branches = BranchManager::new(main.file_io().clone(), main.location().to_owned());
    branches.create_branch("backup").await.unwrap();
    let backup = main.copy_with_branch("backup").await.unwrap();
    assert!(ids(&read).await.is_empty());
    write(&backup, vec![2], vec![20]).await;
    assert_eq!(ids(&read).await, [2]);
    for name in ["", "main", "missing"] {
        let table = main.copy_with_options(HashMap::from([(
            "scan.fallback-branch".into(),
            name.into(),
        )]));
        assert!(
            ids(table.new_read_builder().with_partition_fallback().unwrap())
                .await
                .is_empty()
        );
    }
}

#[tokio::test]
async fn partition_fallback_checks_schema_and_snapshot_scope() {
    let (_temp, catalog, id, main) = setup().await;
    BranchManager::new(main.file_io().clone(), main.location().to_owned())
        .create_branch("backup")
        .await
        .unwrap();
    catalog
        .alter_table(
            &id,
            vec![SchemaChange::rename_column("id".into(), "renamed".into())],
            false,
        )
        .await
        .unwrap();
    let table = catalog.get_table(&id).await.unwrap();
    let mut read = table.new_read_builder();
    read.with_partition_fallback().unwrap();
    let err = read.new_scan().plan().await.unwrap_err();
    assert!(err.to_string().contains("matching field IDs"), "{err}");
    for (key, value) in [
        ("scan.snapshot-id", "1"),
        ("row-tracking.enabled", "true"),
        ("data-evolution.enabled", "true"),
        ("deletion-vectors.enabled", "true"),
        ("query-auth.enabled", "true"),
    ] {
        let table = main.copy_with_options(HashMap::from([(key.into(), value.into())]));
        assert!(
            table.new_read_builder().with_partition_fallback().is_err(),
            "{key}"
        );
    }
    for name in ["unpartitioned", "pk"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("pt", DataType::Int(IntType::new()))
            .option("scan.fallback-branch", "backup");
        let schema = if name == "pk" {
            schema.partition_keys(["pt"]).primary_key(["pt", "id"])
        } else {
            schema
        };
        let id = Identifier::new("db", name);
        catalog
            .create_table(&id, schema.build().unwrap(), false)
            .await
            .unwrap();
        let table = catalog.get_table(&id).await.unwrap();
        assert!(
            table.new_read_builder().with_partition_fallback().is_err(),
            "{name}"
        );
    }
    let pinned = main.copy_with_pinned_snapshot(None);
    assert!(pinned.new_read_builder().with_partition_fallback().is_err());
}

#[tokio::test]
async fn transported_fallback_split_uses_its_source_schema_history() {
    let (_temp, catalog, id, main) = setup().await;
    BranchManager::new(main.file_io().clone(), main.location().to_owned())
        .create_branch("reader")
        .await
        .unwrap();
    let reader = main
        .copy_with_branch("reader")
        .await
        .unwrap()
        .copy_with_options(HashMap::from([(
            "scan.fallback-branch".into(),
            "main".into(),
        )]));
    // Schema 1 exists only on main. Both branches expose the same fields again
    // after the second rename, but reading the old file needs main's schema 1.
    catalog
        .alter_table(
            &id,
            vec![SchemaChange::rename_column("id".into(), "old_id".into())],
            false,
        )
        .await
        .unwrap();
    write(&catalog.get_table(&id).await.unwrap(), vec![42], vec![20]).await;
    catalog
        .alter_table(
            &id,
            vec![SchemaChange::rename_column("old_id".into(), "id".into())],
            false,
        )
        .await
        .unwrap();
    let mut planner = reader.new_read_builder();
    planner.with_partition_fallback().unwrap();
    let plan = planner.new_scan().plan().await.unwrap();
    let split = &plan.splits()[0];
    assert!(split.serialize().is_err());
    let bytes = split.serialize_split_v1().unwrap();
    assert_eq!(i32::from_be_bytes(bytes[12..16].try_into().unwrap()), 6);
    let split = DataSplit::deserialize_split_v1(&bytes).unwrap();
    // The worker has only the transported split and its own configured builder.
    let mut worker = reader.new_read_builder();
    worker.with_partition_fallback().unwrap();
    let read = worker.new_read().unwrap();
    let audit = paimon::table::AuditLogRead::new(read.clone()).unwrap();
    let splits = std::slice::from_ref(&split);
    for result in [read.to_arrow(splits), audit.to_arrow(splits)] {
        let rows: Vec<RecordBatch> = result.unwrap().try_collect().await.unwrap();
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            42
        );
    }
    catalog
        .alter_table(
            &id,
            vec![SchemaChange::rename_column("id".into(), "later_id".into())],
            false,
        )
        .await
        .unwrap();
    let err = planner.new_scan().plan().await.unwrap_err();
    assert!(err.to_string().contains("matching field IDs"), "{err}");
}

#[tokio::test]
async fn fallback_split_keeps_planned_projection_and_predicate_after_ddl() {
    let (_temp, catalog, id, main) = setup().await;
    let reader = create_branch(&main, "reader")
        .await
        .copy_with_options(HashMap::from([(
            "scan.fallback-branch".into(),
            "main".into(),
        )]));
    write(&main, vec![1, 2], vec![20, 20]).await;
    let mut builder = reader.new_read_builder();
    builder
        .with_partition_fallback()
        .unwrap()
        .with_projection(&["pt"])
        .unwrap();
    builder.with_filter(
        PredicateBuilder::new(reader.schema().fields())
            .equal("id", Datum::Int(2))
            .unwrap(),
    );
    let plan = builder.new_scan().plan().await.unwrap();
    let split =
        DataSplit::deserialize_split_v1(&plan.splits()[0].serialize_split_v1().unwrap()).unwrap();
    let stream = builder.new_read().unwrap().to_arrow(&[split]).unwrap();
    catalog
        .alter_table(
            &id,
            vec![
                SchemaChange::rename_column("id".into(), "renamed".into()),
                SchemaChange::add_column("added".into(), DataType::Int(IntType::new())),
            ],
            false,
        )
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        20
    );
    assert!(builder.new_scan().plan().await.is_err());
}

#[tokio::test]
async fn incremental_readers_reject_transported_fallback_splits() {
    use paimon::table::{IncrementalPlan, IncrementalScanMode, IncrementalSplit};
    let (_temp, _, _, main) = setup().await;
    let backup = create_branch(&main, "backup").await;
    write(&backup, vec![42], vec![20]).await;
    let mut builder = main.new_read_builder();
    builder.with_partition_fallback().unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let split =
        DataSplit::deserialize_split_v1(&plan.splits()[0].serialize_split_v1().unwrap()).unwrap();
    let plain = main.new_read_builder().new_read().unwrap();
    for (mode, entry) in [
        (
            IncrementalScanMode::Delta,
            IncrementalSplit::Data(split.clone()),
        ),
        (
            IncrementalScanMode::Diff,
            IncrementalSplit::DiffPair {
                before: vec![],
                after: vec![split],
            },
        ),
    ] {
        let plan = IncrementalPlan::new(mode, vec![entry]);
        for result in [
            plain.to_incremental_arrow(&plan),
            plain.to_audit_log_arrow(&plan),
        ] {
            assert!(
                matches!(result, Err(paimon::Error::Unsupported { message }) if message.contains("partition fallback splits"))
            );
        }
    }
}

#[tokio::test]
async fn fallback_uses_explicit_batch_size_on_both_branches() {
    let (_temp, _, _, main) = setup().await;
    let backup = create_branch(&main, "backup").await;
    write(&main, vec![1, 2, 3, 4], vec![10; 4]).await;
    write(&backup, vec![5, 6, 7, 8], vec![None::<i32>; 4]).await;
    let small = main.copy_with_options(HashMap::from([
        ("read.batch-size".into(), "1".into()),
        ("partition.default-name".into(), "main_only_null".into()),
    ]));
    let mut builder = small.new_read_builder();
    builder.with_partition_fallback().unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![1; 8]
    );
}

#[tokio::test]
async fn missing_fallback_branch_preserves_shard_selection() {
    let (_temp, _, _, main) = setup().await;
    write(&main, vec![1], vec![10]).await;
    let mut builder = main.new_read_builder();
    let ordinary = builder
        .new_scan()
        .with_shard(0, 1)
        .unwrap()
        .plan()
        .await
        .unwrap();
    builder.with_partition_fallback().unwrap();
    let opted_in = builder
        .new_scan()
        .with_shard(0, 1)
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(opted_in.splits(), ordinary.splits());
    create_branch(&main, "backup").await;
    assert!(builder
        .new_scan()
        .with_shard(0, 1)
        .unwrap()
        .plan()
        .await
        .is_err());
}

#[tokio::test]
async fn fallback_planning_spends_limit_only_on_unshadowed_splits() {
    let (_temp, _, _, main) = setup().await;
    let backup = create_branch(&main, "backup").await;
    write(&backup, vec![99, 2, 3, 4], vec![10, 20, 30, 40]).await;
    let mut read = main.new_read_builder();
    read.with_partition_fallback().unwrap().with_limit(1);
    let plan = read.new_scan().plan().await.unwrap();
    assert_eq!(plan.stats().split_candidates_built, 1);
    assert_eq!(plan.splits().len(), 1);
    write(&main, vec![1, 5], vec![10, 10]).await;
    for (limit, candidates) in [(1, 1), (2, 1), (3, 2), (5, 4)] {
        read.with_limit(limit);
        let plan = read.new_scan().plan().await.unwrap();
        assert_eq!(plan.stats().split_candidates_built, candidates);
        assert_eq!(plan.splits().len(), candidates);
        let values = ids(&read).await;
        assert_eq!(values.len(), limit);
        assert!(!values.contains(&99));
    }
    read.with_limit(1).with_filter(
        PredicateBuilder::new(main.schema().fields())
            .equal("id", Datum::Int(4))
            .unwrap(),
    );
    assert_eq!(ids(&read).await, [4]);
}
