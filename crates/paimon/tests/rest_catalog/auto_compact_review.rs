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

fn lookup_overwrite_schema() -> paimon::spec::SchemaBuilder {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "false")
        .option("deletion-vectors.enabled", "true")
        .option("changelog-producer", "lookup")
        .option("pk-bitmap.index.columns", "name")
        .option("write.parquet-buffer-size", "1 B")
}

async fn stage_overlapping_files(builder: &paimon::table::WriteBuilder<'_>) -> Vec<CommitMessage> {
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&people_rows(1..=2)).await.unwrap();
    writer.write_arrow_batch(&people_rows(2..=3)).await.unwrap();
    let inputs = writer.prepare_commit().await.unwrap();
    assert_eq!(inputs[0].new_files.len(), 2);
    inputs
}

#[tokio::test]
async fn lookup_overwrite_recovers_when_published_response_omits_success() {
    let ctx = setup_catalog(vec!["default"]).await;
    let temp = tempfile::tempdir().unwrap();
    let name = "lookup_overwrite";
    let schema = lookup_overwrite_schema()
        .option("commit.max-retries", "0")
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", name, schema, temp.path().to_str().unwrap());
    let identifier = Identifier::new("default", name);
    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    let builder = table.new_write_builder().with_overwrite();
    let messages = stage_overlapping_files(&builder).await;
    let commit = builder.new_commit();
    ctx.server.omit_next_commit_success();
    let error = commit
        .overwrite_with_identifier(messages.clone(), None, 7)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("outcome may be unknown"),
        "{error}"
    );
    commit
        .overwrite_with_identifier(messages, None, 7)
        .await
        .unwrap();

    let reopened = ctx.catalog.get_table(&identifier).await.unwrap();
    let snapshot = reopened
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 1);
    assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
    assert_eq!(
        ids(&read_all(&reopened.new_read_builder()).await.unwrap()),
        [1, 2, 3]
    );
}

#[tokio::test]
async fn lookup_overwrite_preserves_submitted_files_after_a_catalog_conflict() {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    fn files(dir: &Path) -> BTreeSet<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    for snapshot_file_visible in [true, false] {
        let ctx = setup_catalog(vec!["default"]).await;
        let temp = tempfile::tempdir().unwrap();
        let name = "lookup_overwrite";
        ctx.server.add_table_with_schema(
            "default",
            name,
            lookup_overwrite_schema().build().unwrap(),
            temp.path().to_str().unwrap(),
        );
        let table = ctx
            .catalog
            .get_table(&Identifier::new("default", name))
            .await
            .unwrap();
        let competing =
            table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
        let append = competing.new_write_builder();
        let mut writer = append.new_write().unwrap();
        writer.write_arrow_batch(&people_rows(4..=4)).await.unwrap();
        let append_inputs = writer.prepare_commit().await.unwrap();
        let builder = table.new_write_builder().with_overwrite();
        let inputs = stage_overlapping_files(&builder).await;
        let bucket = temp.path().join("bucket-0");
        let staged = files(&bucket);

        let (ready, resume) = ctx.server.pause_next_commit();
        let commit = builder.new_commit();
        let overwrite = commit.overwrite_with_identifier(inputs, None, 7);
        tokio::pin!(overwrite);
        tokio::select! {
            submitted = ready => assert_eq!(submitted.unwrap().id(), 1),
            result = &mut overwrite => panic!("overwrite finished before publication: {result:?}"),
        }
        let data: Vec<_> = files(&bucket).difference(&staged).cloned().collect();
        let indexes = files(&temp.path().join("index"));
        assert_eq!(data.len(), 1);
        assert_eq!(indexes.len(), 1);

        append.new_commit().commit(append_inputs).await.unwrap();
        let manager = table.snapshot_manager();
        let winner = manager.get_snapshot(1).await.unwrap();
        assert_eq!(winner.commit_kind(), &CommitKind::APPEND);
        ctx.server.set_catalog_snapshot("default", name, winner);
        if !snapshot_file_visible {
            manager.delete_snapshot(1).await.unwrap();
        }
        resume.send(()).unwrap();
        overwrite.await.unwrap();

        // The catalog's boolean response does not identify a previous publication.
        assert!(data.iter().chain(&indexes).all(|path| path.is_file()));
        assert!(staged.iter().all(|path| path.exists()));
        let head = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!((head.id(), head.commit_kind()), (2, &CommitKind::OVERWRITE));
        assert_eq!(
            ids(&read_all(&table.new_read_builder()).await.unwrap()),
            [1, 2, 3]
        );
    }
}

#[tokio::test]
async fn lookup_overwrite_preserves_catalog_snapshot_when_rollback_reuses_its_id() {
    let ctx = setup_catalog(vec!["default"]).await;
    let temp = tempfile::tempdir().unwrap();
    let name = "lookup_overwrite";
    ctx.server.add_table_with_schema(
        "default",
        name,
        lookup_overwrite_schema()
            .option("commit.max-retries", "0")
            .build()
            .unwrap(),
        temp.path().to_str().unwrap(),
    );
    let identifier = Identifier::new("default", name);
    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    let seed = table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
    for id in [4, 5] {
        write_batch(&seed, people_rows(id..=id), "seed").await;
    }
    let manager = table.snapshot_manager();
    let old = manager.get_snapshot(2).await.unwrap();
    ctx.server
        .add_rollback_snapshot("default", name, manager.get_snapshot(1).await.unwrap());
    ctx.server
        .set_catalog_snapshot("default", name, old.clone());
    ctx.catalog
        .rollback_to(
            &identifier,
            &paimon::table::Instant::snapshot(1),
            Some(2),
            Some(table.rest_env().unwrap().uuid()),
        )
        .await
        .unwrap();

    let builder = table.new_write_builder().with_overwrite();
    let inputs = stage_overlapping_files(&builder).await;
    let commit = builder.new_commit();
    let (submitted, resume) = ctx.server.pause_next_commit();
    let overwrite = commit.overwrite_with_identifier(inputs.clone(), None, 7);
    tokio::pin!(overwrite);
    let snapshot = tokio::select! {
        submitted = submitted => submitted.unwrap(),
        result = &mut overwrite => panic!("overwrite finished before publication: {result:?}"),
    };
    assert_eq!(snapshot.id(), old.id());
    assert_ne!(snapshot.uuid(), old.uuid());
    // Publish in the catalog only. The replay sees this head and returns false.
    ctx.server
        .set_catalog_snapshot("default", name, snapshot.clone());
    resume.send(()).unwrap();
    let error = overwrite.await.unwrap_err();
    assert!(
        error.to_string().contains("outcome may be unknown"),
        "{error}"
    );
    commit
        .overwrite_with_identifier(inputs, None, 7)
        .await
        .unwrap();

    let reopened = ctx.catalog.get_table(&identifier).await.unwrap();
    assert_eq!(
        manager.get_snapshot(snapshot.id()).await.unwrap().uuid(),
        old.uuid()
    );
    assert_eq!(
        reopened
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .uuid(),
        snapshot.uuid()
    );
    assert_eq!(
        ids(&read_all(&reopened.new_read_builder()).await.unwrap()),
        [1, 2, 3]
    );
    let indexes = paimon::spec::IndexManifest::read(
        reopened.file_io(),
        &manager.manifest_path(snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(indexes.len(), 1);
    assert!(temp
        .path()
        .join("index")
        .join(&indexes[0].index_file.file_name)
        .is_file());
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
