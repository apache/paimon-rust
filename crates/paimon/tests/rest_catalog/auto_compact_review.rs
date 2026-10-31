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
use arrow_array::ArrayRef;
use paimon::spec::CommitKind;
use paimon::table::CommitMessage;

fn schema(evolved: bool) -> Schema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("compaction.min.file-num", "2");
    if evolved {
        builder = builder.column("extra", DataType::Int(IntType::new()));
    }
    builder.build().unwrap()
}

#[tokio::test]
async fn stale_rest_committer_auto_indexes_with_catalog_schema_and_recovers() {
    let ctx = setup_catalog(vec!["default"]).await;
    let temp = tempfile::tempdir().unwrap();
    let name = "rest_auto_index";
    let index_schema = |evolved: bool| {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("write-only", "false")
            .option("global-index.btree.index-columns", "id");
        if evolved {
            builder = builder.column("extra", DataType::Int(IntType::new()));
        }
        builder.build().unwrap()
    };
    ctx.server.add_table_with_schema(
        "default",
        name,
        index_schema(false),
        temp.path().to_str().unwrap(),
    );
    let identifier = Identifier::new("default", name);
    let stale = ctx.catalog.get_table(&identifier).await.unwrap();
    ctx.server
        .set_table_schema_id("default", name, index_schema(true), 1);
    let current = ctx.catalog.get_table(&identifier).await.unwrap();
    let commit = stale.new_write_builder().new_commit();
    let messages = stage(&current, 1, true).await;
    commit
        .commit_with_identifier(messages.clone(), 7)
        .await
        .unwrap();
    commit
        .filter_and_commit_with_identifier(messages, 7)
        .await
        .unwrap();
    let snapshot = current
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 2);
    assert_eq!(snapshot.schema_id(), 1);
    assert!(snapshot.index_manifest().is_some());
    assert!(current.schema_manager().latest().await.unwrap().is_none());
    assert!(current
        .new_global_index_build_builder()
        .with_index_column("id")
        .build()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(read_rows(&current).await, vec![(1, 10, Some(100))]);
}

async fn catalog_only_tables(name: &str) -> (TestContext, tempfile::TempDir, Table, Table) {
    let ctx = setup_catalog(vec!["default"]).await;
    let temp = tempfile::tempdir().unwrap();
    let location = temp.path().to_str().unwrap();
    ctx.server
        .add_table_with_schema("default", name, schema(false), location);
    let identifier = Identifier::new("default", name);
    let stale = ctx.catalog.get_table(&identifier).await.unwrap();
    assert_eq!(stale.schema().id(), 0);
    ctx.server
        .set_table_schema_id("default", name, schema(true), 1);
    let current = ctx.catalog.get_table(&identifier).await.unwrap();
    assert_eq!(current.schema().id(), 1);
    assert!(current.schema_manager().latest().await.unwrap().is_none());
    (ctx, temp, stale, current)
}

async fn stage(table: &Table, id: i32, evolved: bool) -> Vec<CommitMessage> {
    let mut columns = vec![
        ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        (
            "value",
            Arc::new(Int32Array::from(vec![id * 10])) as ArrayRef,
        ),
    ];
    if evolved {
        columns.push((
            "extra",
            Arc::new(Int32Array::from(vec![id * 100])) as ArrayRef,
        ));
    }
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn read_rows(table: &Table) -> Vec<(i32, i32, Option<i32>)> {
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
    let mut rows = Vec::new();
    for batch in batches {
        let ids = column::<Int32Array>(&batch, "id");
        let values = column::<Int32Array>(&batch, "value");
        let extras = column::<Int32Array>(&batch, "extra");
        rows.extend((0..batch.num_rows()).map(|index| {
            (
                ids.value(index),
                values.value(index),
                extras.is_valid(index).then(|| extras.value(index)),
            )
        }));
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn stale_rest_writer_can_publish_without_filesystem_schema_metadata() {
    let (_ctx, _temp, stale, current) = catalog_only_tables("stale_auto_writer").await;
    let messages = stage(&stale, 1, false).await;
    assert!(messages
        .iter()
        .flat_map(|message| &message.new_files)
        .all(|file| file.schema_id == 0));
    stale
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    let snapshot = current
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 1);
    assert_eq!(snapshot.schema_id(), 1);
    assert_eq!(snapshot.commit_kind(), &CommitKind::APPEND);
    assert_eq!(snapshot.total_record_count(), Some(1));
    assert!(current.schema_manager().latest().await.unwrap().is_none());
    assert_eq!(stale.schema().id(), 0);
}

#[tokio::test]
async fn stale_rest_committer_compacts_using_the_catalog_schema() {
    let (_ctx, _temp, stale, current) = catalog_only_tables("stale_auto_committer").await;
    let commit = stale.new_write_builder().new_commit();
    for id in [1, 2] {
        commit
            .commit(stage(&current, id, true).await)
            .await
            .unwrap();
    }
    let snapshot = current
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 3);
    assert_eq!(snapshot.schema_id(), 1);
    assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(snapshot.total_record_count(), Some(2));
    assert!(current.schema_manager().latest().await.unwrap().is_none());
    assert_eq!(stale.schema().id(), 0);
    let builder = current.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    assert_eq!(
        plan.splits()
            .iter()
            .flat_map(|split| split.data_files())
            .count(),
        1
    );
    assert!(plan
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .all(|file| file.schema_id == 1));
    assert_eq!(
        read_rows(&current).await,
        vec![(1, 10, Some(100)), (2, 20, Some(200))]
    );
}

#[tokio::test]
async fn stale_rest_committer_compacts_mixed_known_schemas_without_schema_files() {
    for write_only in [true, false] {
        let name = format!("mixed_auto_schemas_{write_only}");
        let (_ctx, _temp, stale, current) = catalog_only_tables(&name).await;
        let stale = stale.copy_with_options(HashMap::from([(
            "write-only".into(),
            write_only.to_string(),
        )]));
        let commit = stale.new_write_builder().new_commit();
        let original = stage(&stale, 1, false).await;
        assert!(original
            .iter()
            .flat_map(|message| &message.new_files)
            .all(|file| file.schema_id == 0));
        commit.commit(original).await.unwrap();
        let evolved = stage(&current, 2, true).await;
        assert!(evolved
            .iter()
            .flat_map(|message| &message.new_files)
            .all(|file| file.schema_id == 1));
        commit.commit(evolved).await.unwrap();
        let snapshot = current
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.schema_id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(2));
        assert!(current.schema_manager().latest().await.unwrap().is_none());
        assert_eq!(stale.schema().id(), 0);
        if write_only {
            // Ordinary publication does not require historical schema files.
            // Existing explicit reads of the unmerged old file may need them.
            assert_eq!(snapshot.id(), 2);
            assert_eq!(snapshot.commit_kind(), &CommitKind::APPEND);
        } else {
            assert_eq!(snapshot.id(), 3);
            assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
            assert_eq!(
                read_rows(&current).await,
                vec![(1, 10, None), (2, 20, Some(200))]
            );
        }
    }
}

#[tokio::test]
async fn dynamic_dv_override_does_not_change_the_cached_catalog_schema() {
    let ctx = setup_catalog(vec!["default"]).await;
    let temp = tempfile::tempdir().unwrap();
    let name = "raw_schema_options";
    let stored = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", name, stored, temp.path().to_str().unwrap());
    let table = ctx
        .catalog
        .get_table(&Identifier::new("default", name))
        .await
        .unwrap();
    let write_only = table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
    write_only
        .new_write_builder()
        .new_commit()
        .commit(stage(&write_only, 1, false).await)
        .await
        .unwrap();

    // A dynamic setting cannot turn the catalog's old non-DV files into
    // historical DV files. validate_dv_schemas must still reject that rewrite.
    let changed = table.copy_with_options(HashMap::from([
        ("write-only".into(), "false".into()),
        ("deletion-vectors.enabled".into(), "true".into()),
        ("compaction.force-up-level-0".into(), "true".into()),
    ]));
    changed
        .new_write_builder()
        .new_commit()
        .commit(stage(&changed, 2, false).await)
        .await
        .unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 2);
    assert_eq!(snapshot.commit_kind(), &CommitKind::APPEND);
    assert_eq!(snapshot.total_record_count(), Some(2));
    assert!(table.schema_manager().latest().await.unwrap().is_none());
    assert!(!table
        .schema_manager()
        .schema(0)
        .await
        .unwrap()
        .core_options()
        .deletion_vectors_enabled());
}
