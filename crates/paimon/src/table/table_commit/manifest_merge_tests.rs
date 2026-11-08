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

/// The sorted (file name, level) pairs that are live in the latest snapshot,
/// first as the scan resolves them, ignoring entry order, then as the ordered
/// `active_entries` does.
async fn live_files_by_reader(
    commit: &TableCommit,
    io: &FileIO,
    path: &str,
) -> [Vec<(String, i32)>; 2] {
    let snapshot = latest_snapshot(io, path).await.unwrap();
    let scanned = TableScan::new(&commit.table, None, vec![], None, None, None)
        .with_scan_all_files()
        .plan_manifest_entries(&snapshot)
        .await
        .unwrap();
    let listed = active_entries(io, path, &snapshot).await;
    [scanned, listed].map(|entries| {
        let mut files = entries
            .iter()
            .map(|entry| (entry.file().file_name.clone(), entry.file().level))
            .collect::<Vec<_>>();
        files.sort();
        files
    })
}

/// Assert that the scan lists `expected[0]` and the ordered reader
/// `expected[1]`, each with `unrelated.parquet` added.
async fn assert_live_files_plus_unrelated(
    commit: &TableCommit,
    io: &FileIO,
    path: &str,
    expected: [Vec<(String, i32)>; 2],
) {
    let actual = live_files_by_reader(commit, io, path).await;
    for ((mut expected, actual), reader) in
        expected.into_iter().zip(actual).zip(["scan", "ordered"])
    {
        expected.push(("unrelated.parquet".to_string(), 0));
        expected.sort();
        assert_eq!(actual, expected, "{reader} reader");
    }
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
async fn group_that_deletes_its_own_add_keeps_only_the_delete() {
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
    assert_eq!(base.len(), 1, "{base:?}");
    assert_eq!(
        (base[0].num_added_files(), base[0].num_deleted_files()),
        (0, 1)
    );
    assert_eq!(
        active_names(&io, path).await,
        HashSet::from(["y.parquet".to_string()])
    );
}

#[tokio::test]
async fn merging_an_add_with_its_delete_keeps_the_delete_of_a_re_added_file() {
    // [ADD old, DELETE old] merges while a later manifest adds old.parquet
    // again. Dropping the DELETE would make that ADD live for the scan.
    let io = test_file_io();
    let path = "memory:/manifest-merge-delete-before-re-add";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(&io, path, &[("manifest.merge-min-count", "1000")]);
    let re_add = replacement_message(&[], "old.parquet");
    let [retained, deletion, re_add] =
        commit_retained_deletion_replacement(&io, path, &mut commit, &[], re_add).await;
    // [retained, deletion] reaches the target and merges; re_add stays alone.
    commit.manifest_target_size = retained.file_size() + deletion.file_size();
    let before = live_files_by_reader(&commit, &io, path).await;
    let old = ("old.parquet".to_string(), 0);
    assert!(!before[0].contains(&old) && before[1].contains(&old));
    commit
        .commit(vec![append_message("unrelated.parquet")])
        .await
        .unwrap();

    assert_live_files_plus_unrelated(&commit, &io, path, before).await;
    let base = base_manifests(&io, path).await;
    let names = base
        .iter()
        .map(ManifestFileMeta::file_name)
        .collect::<Vec<_>>();
    assert!(!names.contains(&retained.file_name()), "{names:?}");
    assert!(!names.contains(&deletion.file_name()), "{names:?}");
    assert_eq!(names.last(), Some(&re_add.file_name()));
}

#[tokio::test]
async fn merging_a_repeated_add_with_its_delete_keeps_the_first_add_deleted() {
    // old.parquet is added twice and then deleted. Merging the second ADD
    // with the DELETE must keep the DELETE, or the first ADD becomes live.
    let io = test_file_io();
    let path = "memory:/manifest-merge-add-twice-then-delete";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    // A one-byte target closes a group after every manifest, so these
    // commits merge nothing.
    commit.manifest_target_size = 1;
    let old_file = test_data_file("old.parquet", 1);
    let mut initial = vec![old_file.clone()];
    // Random names compress poorly, so this manifest stays larger than the
    // other two combined.
    initial.extend(
        (0..128).map(|_| test_data_file(&format!("filler-{}.parquet", uuid::Uuid::new_v4()), 1)),
    );
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(old_file);
    let messages = [
        CommitMessage::new(vec![], 0, initial),
        replacement_message(&[], "old.parquet"),
        deletion,
    ];
    for message in messages {
        commit.commit(vec![message]).await.unwrap();
    }
    let mut manifests = base_manifests(&io, path).await;
    manifests.extend(delta_manifests(&io, path).await);
    let [first, second, deletion]: [ManifestFileMeta; 3] = manifests.try_into().unwrap();
    // first closes its own group; [second, deletion] is the trailing group.
    commit.manifest_target_size = second.file_size() + deletion.file_size() + 1;
    assert!(first.file_size() >= commit.manifest_target_size);
    let before = live_files_by_reader(&commit, &io, path).await;
    commit
        .commit(vec![append_message("unrelated.parquet")])
        .await
        .unwrap();

    assert_live_files_plus_unrelated(&commit, &io, path, before).await;
    let base = base_manifests(&io, path).await;
    assert_eq!(base.len(), 2);
    assert_eq!(base[0].file_name(), first.file_name());
    assert_eq!(
        (base[1].num_added_files(), base[1].num_deleted_files()),
        (0, 1)
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
        // An unpublished first attempt leaves a retry, which does not merge.
        assert_eq!(base.len(), if publish_first { 1 } else { 2 });
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
async fn group_that_adds_a_file_twice_without_deleting_it_stays_unmerged() {
    // Failing here would fail this commit and every later one.
    let io = test_file_io();
    let path = "memory:/manifest-merge-add-twice";
    setup_dirs(&io, path).await;
    let commit = merge_table(&io, path, &[("manifest.merge-min-count", "3")]);
    for name in ["x.parquet", "x.parquet", "z.parquet"] {
        commit.commit(vec![append_message(name)]).await.unwrap();
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
async fn group_that_re_adds_a_file_it_deletes_drops_the_re_add() {
    // [DELETE old, ADD old] merges to [DELETE old]. Scans already treat
    // old.parquet as deleted, and the ordered reader then agrees with them.
    let io = test_file_io();
    let path = "memory:/manifest-merge-re-add";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    let re_add = replacement_message(&[], "old.parquet");
    let [retained, deletion, re_add] =
        commit_retained_deletion_replacement(&io, path, &mut commit, &[], re_add).await;
    commit.manifest_target_size = deletion.file_size() + re_add.file_size() + 1;
    assert!(retained.file_size() >= commit.manifest_target_size);
    let [scanned, listed] = live_files_by_reader(&commit, &io, path).await;
    let old = ("old.parquet".to_string(), 0);
    assert!(!scanned.contains(&old) && listed.contains(&old));

    commit
        .commit(vec![append_message("unrelated.parquet")])
        .await
        .unwrap();
    assert_live_files_plus_unrelated(&commit, &io, path, [scanned.clone(), scanned]).await;
    let base = base_manifests(&io, path).await;
    assert_eq!(base.len(), 2);
    assert_eq!(base[0].file_name(), retained.file_name());
    assert_eq!(
        (base[1].num_added_files(), base[1].num_deleted_files()),
        (0, 1)
    );
}

#[tokio::test]
async fn re_added_file_does_not_keep_later_groups_unmerged() {
    // After [ADD x, DELETE x] merges to [DELETE x], each trailing group holds
    // that DELETE. A group holding it and the re-ADD of x must still merge,
    // or the base list grows by one manifest per commit.
    let io = test_file_io();
    let path = "memory:/manifest-merge-re-add-growth";
    setup_dirs(&io, path).await;
    let commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    for message in [
        append_message("x.parquet"),
        deletion,
        append_message("y.parquet"),
        append_message("x.parquet"),
    ] {
        commit.commit(vec![message]).await.unwrap();
        assert!(base_manifests(&io, path).await.len() <= 1);
    }
    // Scans treat the re-added x.parquet as deleted. The ordered reader lists
    // it until a merge drops the re-ADD.
    let [mut scanned, listed] = live_files_by_reader(&commit, &io, path).await;
    let (x, y) = (("x.parquet".to_string(), 0), ("y.parquet".to_string(), 0));
    assert_eq!(scanned, std::slice::from_ref(&y));
    assert_eq!(listed, [x, y]);

    for id in 0..12 {
        let name = format!("data-{id}.parquet");
        commit.commit(vec![append_message(&name)]).await.unwrap();
        let base = base_manifests(&io, path).await;
        assert!(base.len() <= 1, "commit {id}: {base:?}");
        scanned.push((name, 0));
        scanned.sort();
        let expected = [scanned.clone(), scanned.clone()];
        assert_eq!(
            live_files_by_reader(&commit, &io, path).await,
            expected,
            "commit {id}"
        );
    }
}

/// Rewrite a manifest as a newer writer with one more `_FILE` field would.
async fn rewrite_with_unknown_field(io: &FileIO, path: &str, manifest: &ManifestFileMeta) {
    let manifest_path = format!("{path}/manifest/{}", manifest.file_name());
    let entries = Manifest::read(io, &manifest_path).await.unwrap();
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
    io.new_output(&manifest_path)
        .unwrap()
        .write(writer.into_inner().unwrap().into())
        .await
        .unwrap();
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
    let newer = delta_manifests(&io, path).await.remove(0);
    rewrite_with_unknown_field(&io, path, &newer).await;

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

struct RejectFirstPublication {
    manager: SnapshotManager,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl SnapshotCommit for RejectFirstPublication {
    async fn commit(
        &self,
        _: Option<&str>,
        snapshot: &Snapshot,
        _: &str,
        _: &[PartitionStatistics],
    ) -> Result<bool> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            return Ok(false);
        }
        self.manager.commit_snapshot(snapshot).await
    }
}

#[tokio::test]
async fn commit_retry_carries_manifests_forward_without_merging() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-retry";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    commit.commit_min_retry_wait_ms = 0;
    commit.commit_max_retry_wait_ms = 0;
    for name in ["first", "second"] {
        commit.commit(vec![append_message(name)]).await.unwrap();
    }
    let mut carried = base_manifests(&io, path).await;
    carried.extend(delta_manifests(&io, path).await);
    assert_eq!(carried.len(), 2);

    commit.snapshot_commit = Arc::new(RejectFirstPublication {
        manager: commit.snapshot_manager.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let before = manifest_paths(&io, path).await;
    commit.commit(vec![append_message("third")]).await.unwrap();
    // The retry carries both manifests forward.
    assert_eq!(base_manifests(&io, path).await, carried);
    // The rejected first attempt merged them. Each attempt also left a delta
    // manifest holding "third", so the merged one is found by its content.
    let mut new_manifests = Vec::new();
    for manifest_path in manifest_paths(&io, path).await.difference(&before) {
        let name = manifest_path.rsplit('/').next().unwrap();
        if name.starts_with("manifest-") && !name.starts_with("manifest-list-") {
            let mut files = Manifest::read(&io, manifest_path)
                .await
                .unwrap()
                .iter()
                .map(|entry| entry.file().file_name.clone())
                .collect::<Vec<_>>();
            files.sort();
            new_manifests.push(files);
        }
    }
    assert!(
        new_manifests.contains(&vec!["first".to_string(), "second".to_string()]),
        "{new_manifests:?}"
    );

    commit.commit(vec![append_message("fourth")]).await.unwrap();
    assert_eq!(base_manifests(&io, path).await.len(), 1);
}

#[tokio::test]
async fn write_only_commits_skip_merging_when_configured() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-skip-write-only";
    setup_dirs(&io, path).await;
    // test_table_with_options sets write-only=true unless a test overrides it.
    let commit = merge_table(
        &io,
        path,
        &[
            ("manifest.merge-min-count", "2"),
            ("manifest.merge.skip-on-write-only", "true"),
        ],
    );
    for name in ["first", "second", "third"] {
        commit.commit(vec![append_message(name)]).await.unwrap();
    }
    assert_eq!(base_manifests(&io, path).await.len(), 2);
}

#[tokio::test]
async fn skip_on_write_only_does_not_apply_to_write_enabled_tables() {
    let io = test_file_io();
    let path = "memory:/manifest-merge-skip-write-enabled";
    setup_dirs(&io, path).await;
    // Commit through a write-only table: a write-enabled commit would start
    // compaction on data files that do not exist.
    let writer = merge_table(&io, path, &[]);
    for name in ["first", "second"] {
        writer.commit(vec![append_message(name)]).await.unwrap();
    }
    let mut input = base_manifests(&io, path).await;
    input.extend(delta_manifests(&io, path).await);
    assert_eq!(input.len(), 2);

    let merger = merge_table(
        &io,
        path,
        &[
            ("write-only", "false"),
            ("manifest.merge-min-count", "2"),
            ("manifest.merge.skip-on-write-only", "true"),
        ],
    );
    let mut created = Vec::new();
    let merged = merger.merge_manifests(input, &mut created).await.unwrap();
    assert_eq!(merged.len(), 1);
    assert!(!created.is_empty());
}
