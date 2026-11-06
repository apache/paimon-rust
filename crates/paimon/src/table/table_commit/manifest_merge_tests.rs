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

fn merge_table(io: &FileIO, path: &str, options: &[(&str, &str)]) -> TableCommit {
    let options = options
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    TableCommit::new(
        test_table_with_options(io, path, options),
        "test-user".into(),
    )
}

async fn base_manifests(io: &FileIO, path: &str) -> Vec<ManifestFileMeta> {
    let snapshot = latest_snapshot(io, path).await.unwrap();
    ManifestList::read(
        io,
        &format!("{path}/manifest/{}", snapshot.base_manifest_list()),
    )
    .await
    .unwrap()
}

async fn delta_manifests(io: &FileIO, path: &str) -> Vec<ManifestFileMeta> {
    let snapshot = latest_snapshot(io, path).await.unwrap();
    ManifestList::read(
        io,
        &format!("{path}/manifest/{}", snapshot.delta_manifest_list()),
    )
    .await
    .unwrap()
}

async fn active_names(io: &FileIO, path: &str) -> HashSet<String> {
    let snapshot = latest_snapshot(io, path).await.unwrap();
    active_entries(io, path, &snapshot)
        .await
        .into_iter()
        .map(|entry| entry.file().file_name.clone())
        .collect()
}

#[tokio::test]
async fn commit_merges_thirty_small_manifests_like_java() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-min-count";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    for id in 0..30 {
        let name = format!("data-{id}.parquet");
        commit.commit(vec![append_message(&name)]).await.unwrap();
        assert_eq!(base_manifests(&io, path).await.len(), id, "commit {id}");
    }
    // The 31st commit carries 30 small manifests forward, which reaches
    // Java's default manifest.merge-min-count.
    commit
        .commit(vec![append_message("data-30.parquet")])
        .await
        .unwrap();
    let base = base_manifests(&io, path).await;
    assert_eq!(base.len(), 1);
    assert_eq!(base[0].num_added_files(), 30);
    assert_eq!(active_names(&io, path).await.len(), 31);
}

/// Commits `old.parquet` among 128 fillers in `old_partition`, then its DELETE
/// and `replacement`, each in a manifest of its own. Returns the three
/// manifests the next commit sees, in order.
async fn commit_retained_deletion_replacement(
    io: &FileIO,
    path: &str,
    commit: &mut TableCommit,
    old_partition: &[u8],
    replacement: CommitMessage,
) -> [ManifestFileMeta; 3] {
    let old_file = test_data_file("old.parquet", 1);
    let mut initial = vec![old_file.clone()];
    // Random names compress poorly, so this manifest stays larger than the
    // other two combined.
    initial.extend(
        (0..128).map(|_| test_data_file(&format!("filler-{}.parquet", uuid::Uuid::new_v4()), 1)),
    );
    commit
        .commit(vec![CommitMessage::new(old_partition.to_vec(), 0, initial)])
        .await
        .unwrap();
    // A one-byte target closes a group after every manifest, so the next two
    // commits merge nothing.
    commit.manifest_target_size = 1;
    let mut deletion = CommitMessage::new(old_partition.to_vec(), 0, vec![]);
    deletion.deleted_files.push(old_file);
    commit.commit(vec![deletion]).await.unwrap();
    commit.commit(vec![replacement]).await.unwrap();

    let mut manifests = base_manifests(io, path).await;
    manifests.extend(delta_manifests(io, path).await);
    manifests.try_into().unwrap()
}

fn replacement_message(partition: &[u8], name: &str) -> CommitMessage {
    CommitMessage::new(partition.to_vec(), 0, vec![test_data_file(name, 1)])
}

#[tokio::test]
async fn minor_merge_keeps_a_delete_whose_add_stays_in_a_retained_manifest() {
    // #899: the old Rust minor merge resolved each group as a complete active
    // set, so this DELETE disappeared and old.parquet became live again.
    let io = test_file_io();
    let path = "memory:/manifest-merge-retained-add";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    let replacement = replacement_message(&[], "replacement.parquet");
    let [retained, deletion, replacement] =
        commit_retained_deletion_replacement(&io, path, &mut commit, &[], replacement).await;
    // The retained manifest closes its own group; the other two form the
    // trailing group and merge.
    commit.manifest_target_size = deletion.file_size() + replacement.file_size() + 1;
    assert!(retained.file_size() >= commit.manifest_target_size);
    commit
        .commit(vec![replacement_message(&[], "unrelated.parquet")])
        .await
        .unwrap();

    let base = base_manifests(&io, path).await;
    assert_eq!(base.len(), 2);
    assert_eq!(base[0].file_name(), retained.file_name());
    assert_eq!(
        (base[1].num_added_files(), base[1].num_deleted_files()),
        (1, 1)
    );
    let live = active_names(&io, path).await;
    assert_eq!(live.len(), 130);
    assert!(!live.contains("old.parquet"));
    assert!(live.contains("replacement.parquet"));
}

#[tokio::test]
async fn group_reaching_exactly_the_target_size_merges_below_min_count() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-exact-target";
    setup_dirs(&io, path).await;
    let mut commit = setup_commit(&io, path);
    let replacement = replacement_message(&[], "replacement.parquet");
    let [retained, deletion, replacement] =
        commit_retained_deletion_replacement(&io, path, &mut commit, &[], replacement).await;
    // A group closes once its size reaches the target and then merges, however
    // far it is below the default manifest.merge-min-count of 30.
    commit.manifest_target_size = deletion.file_size() + replacement.file_size();
    assert!(retained.file_size() >= commit.manifest_target_size);
    commit
        .commit(vec![append_message("unrelated.parquet")])
        .await
        .unwrap();

    let base = base_manifests(&io, path).await;
    assert_eq!(base.len(), 2);
    assert_eq!(base[0].file_name(), retained.file_name());
    assert_eq!(
        (base[1].num_added_files(), base[1].num_deleted_files()),
        (1, 1)
    );
}

#[tokio::test]
async fn fully_cancelled_group_leaves_the_base_list() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-cancelled";
    setup_dirs(&io, path).await;
    let commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    commit
        .commit(vec![append_message("x.parquet")])
        .await
        .unwrap();
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    commit.commit(vec![deletion]).await.unwrap();
    commit
        .commit(vec![append_message("y.parquet")])
        .await
        .unwrap();

    let base = base_manifests(&io, path).await;
    assert!(base.is_empty(), "{base:?}");
    assert_eq!(
        active_names(&io, path).await,
        HashSet::from(["y.parquet".to_string()])
    );
}

#[tokio::test]
async fn failed_merge_removes_manifests_written_for_earlier_groups() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-prepare-failure";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(
        &io,
        path,
        &[
            ("manifest.merge-min-count", "2"),
            ("manifest.sidecar.enabled", "true"),
        ],
    );
    commit.manifest_target_size = 1;
    // Random names compress poorly, so each large manifest outweighs the two
    // small ones together.
    for count in [256, 256, 1, 1] {
        let files = (0..count)
            .map(|_| test_data_file(&format!("data-{}.parquet", uuid::Uuid::new_v4()), 1))
            .collect();
        commit
            .commit(vec![CommitMessage::new(vec![], 0, files)])
            .await
            .unwrap();
    }
    let mut manifests = base_manifests(&io, path).await;
    manifests.extend(delta_manifests(&io, path).await);
    let [large, _, small, missing]: [ManifestFileMeta; 4] = manifests.try_into().unwrap();
    // The two large manifests close a group and merge first; the trailing
    // group of two small ones then fails to read.
    commit.manifest_target_size = large.file_size() + 1;
    assert!(small.file_size() + missing.file_size() < commit.manifest_target_size);
    let before = manifest_paths(&io, path).await;
    io.delete_file(&format!("{path}/manifest/{}", missing.file_name()))
        .await
        .unwrap();
    let remaining = manifest_paths(&io, path).await;
    assert_eq!(remaining.len(), before.len() - 1);

    let error = commit
        .commit(vec![append_message("pending.parquet")])
        .await
        .unwrap_err();
    assert!(error.to_string().contains(missing.file_name()), "{error}");
    assert_eq!(manifest_paths(&io, path).await, remaining);
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 4);
}

#[tokio::test]
async fn merged_manifests_survive_an_uncertain_publication() {
    for publish_first in [false, true] {
        let io = test_file_io();
        let path = format!("memory:/manifest-merge-lost-response-{publish_first}");
        setup_dirs(&io, &path).await;
        let mut commit = merge_table(&io, &path, &[("manifest.merge-min-count", "2")]);
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        for name in ["data-0.parquet", "data-1.parquet"] {
            commit.commit(vec![append_message(name)]).await.unwrap();
        }
        let publisher = Arc::new(LostResponseCommit {
            manager: commit.snapshot_manager.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            publish_first,
        });
        commit.snapshot_commit = publisher.clone();
        commit
            .commit(vec![append_message("data-2.parquet")])
            .await
            .unwrap();

        assert_eq!(
            publisher.calls.load(std::sync::atomic::Ordering::SeqCst),
            if publish_first { 1 } else { 2 }
        );
        let base = base_manifests(&io, &path).await;
        assert_eq!(base.len(), 1);
        for manifest in &base {
            let manifest_path = format!("{path}/manifest/{}", manifest.file_name());
            assert!(io.exists(&manifest_path).await.unwrap(), "{manifest_path}");
        }
        assert_eq!(active_names(&io, &path).await.len(), 3);
    }
}

fn partitioned_merge_table(io: &FileIO, path: &str, options: &[(&str, &str)]) -> TableCommit {
    let mut builder = crate::spec::Schema::builder()
        .column(
            "pt",
            DataType::VarChar(crate::spec::VarCharType::string_type()),
        )
        .column("id", DataType::Int(crate::spec::IntType::new()))
        .partition_keys(["pt"])
        .option("write-only", "true");
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "test_table"),
        path.to_string(),
        TableSchema::new(0, &builder.build().unwrap()),
        None,
    );
    TableCommit::new(table, "test-user".into())
}

#[tokio::test]
async fn partition_filtered_scan_sees_a_merged_delete() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-partition-filter";
    setup_dirs(&io, path).await;
    let mut commit = partitioned_merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    let (a, b) = (partition_bytes("a"), partition_bytes("b"));
    let replacement = replacement_message(&b, "replacement.parquet");
    let [retained, deletion, replacement] =
        commit_retained_deletion_replacement(&io, path, &mut commit, &a, replacement).await;
    commit.manifest_target_size = deletion.file_size() + replacement.file_size() + 1;
    assert!(retained.file_size() >= commit.manifest_target_size);
    commit
        .commit(vec![replacement_message(&b, "unrelated.parquet")])
        .await
        .unwrap();

    let base = base_manifests(&io, path).await;
    assert_eq!(base.len(), 2);
    let merged = Manifest::read(&io, &format!("{path}/manifest/{}", base[1].file_name()))
        .await
        .unwrap();
    let in_a = merged
        .iter()
        .filter(|entry| entry.partition() == a.as_slice())
        .map(|entry| (*entry.kind(), entry.file().file_name.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(in_a, [(FileKind::Delete, "old.parquet")]);

    // The merged manifest's partition stats must cover its DELETE, or this
    // scan skips the manifest and old.parquet comes back.
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    let filter = partition_filter_for(&commit, vec![a]);
    let entries = TableScan::new(&commit.table, Some(filter), vec![], None, None, None)
        .with_scan_all_files()
        .plan_manifest_entries(&snapshot)
        .await
        .unwrap();
    let names = entries
        .iter()
        .map(|entry| entry.file().file_name.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(names.len(), 128);
    assert!(!names.contains("old.parquet"));
}

#[tokio::test]
async fn group_that_adds_a_file_twice_stays_unmerged() {
    // Failing here would fail this commit and every later one.
    let io = test_file_io();
    let path = "memory:/manifest-merge-add-twice";
    setup_dirs(&io, path).await;
    let commit = merge_table(&io, path, &[("manifest.merge-min-count", "3")]);
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    for message in [
        append_message("x.parquet"),
        deletion,
        append_message("x.parquet"),
    ] {
        commit.commit(vec![message]).await.unwrap();
    }
    let mut previous = base_manifests(&io, path).await;
    previous.extend(delta_manifests(&io, path).await);
    let mut live = active_names(&io, path).await;

    commit
        .commit(vec![append_message("y.parquet")])
        .await
        .unwrap();
    assert_eq!(base_manifests(&io, path).await, previous);
    live.insert("y.parquet".into());
    assert_eq!(active_names(&io, path).await, live);
}

#[tokio::test]
async fn group_that_re_adds_a_file_it_deletes_stays_unmerged() {
    // Cancelling the DELETE against the later ADD would leave the retained ADD
    // of old.parquet without its DELETE.
    let io = test_file_io();
    let path = "memory:/manifest-merge-re-add";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    let re_add = replacement_message(&[], "old.parquet");
    let previous = commit_retained_deletion_replacement(&io, path, &mut commit, &[], re_add).await;
    let [retained, deletion, re_add] = &previous;
    commit.manifest_target_size = deletion.file_size() + re_add.file_size() + 1;
    assert!(retained.file_size() >= commit.manifest_target_size);
    let mut live = active_names(&io, path).await;

    commit
        .commit(vec![append_message("unrelated.parquet")])
        .await
        .unwrap();
    assert_eq!(base_manifests(&io, path).await, previous);
    live.insert("unrelated.parquet".into());
    assert_eq!(active_names(&io, path).await, live);
}

#[tokio::test]
async fn manifest_with_an_unknown_field_stays_unmerged() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-unknown-field";
    setup_dirs(&io, path).await;
    let commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    commit
        .commit(vec![append_message("data-0.parquet")])
        .await
        .unwrap();
    // Rewrite the manifest as a newer writer with one more `_FILE` field would.
    let newer = delta_manifests(&io, path).await.remove(0);
    let newer_path = format!("{path}/manifest/{}", newer.file_name());
    let entries = Manifest::read(&io, &newer_path).await.unwrap();
    let file_name_field = r#"{"name": "_FILE_NAME", "type": "string"},"#;
    let schema_json = MANIFEST_ENTRY_SCHEMA.replacen(
        file_name_field,
        &format!(
            r#"{file_name_field} {{"name": "_NEW", "type": ["null", "long"], "default": null}},"#
        ),
        1,
    );
    assert_ne!(schema_json, MANIFEST_ENTRY_SCHEMA);
    let schema = apache_avro::Schema::parse_str(&schema_json).unwrap();
    let mut writer = crate::spec::new_avro_writer(
        &schema,
        crate::spec::DEFAULT_AVRO_COMPRESSION,
        crate::spec::DEFAULT_AVRO_BLOCK_SIZE,
    )
    .unwrap();
    for entry in &entries {
        let value = apache_avro::to_value(entry)
            .unwrap()
            .resolve(&schema)
            .unwrap();
        writer.append(value).unwrap();
    }
    io.new_output(&newer_path)
        .unwrap()
        .write(writer.into_inner().unwrap().into())
        .await
        .unwrap();

    commit
        .commit(vec![append_message("data-1.parquet")])
        .await
        .unwrap();
    let mut previous = base_manifests(&io, path).await;
    previous.extend(delta_manifests(&io, path).await);
    commit
        .commit(vec![append_message("data-2.parquet")])
        .await
        .unwrap();
    assert_eq!(base_manifests(&io, path).await, previous);
    assert_eq!(previous[0].file_name(), newer.file_name());
    assert_eq!(active_names(&io, path).await.len(), 3);
}
