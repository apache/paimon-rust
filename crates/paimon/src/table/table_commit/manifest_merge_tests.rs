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

#[tokio::test]
async fn minor_merge_keeps_a_delete_whose_add_stays_in_a_retained_manifest() {
    // #899: the old Rust minor merge resolved each group as a complete active
    // set, so this DELETE disappeared and old.parquet became live again.
    let io = test_file_io();
    let path = "memory:/manifest-merge-retained-add";
    setup_dirs(&io, path).await;
    let mut commit = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    let old_file = test_data_file("old.parquet", 1);
    let mut initial = vec![old_file.clone()];
    // Random names compress poorly, so this manifest stays larger than the
    // other two combined.
    initial.extend(
        (0..128).map(|_| test_data_file(&format!("filler-{}.parquet", uuid::Uuid::new_v4()), 1)),
    );
    commit
        .commit(vec![CommitMessage::new(vec![], 0, initial)])
        .await
        .unwrap();
    // A one-byte target closes a group after every manifest, so the next two
    // commits merge nothing.
    commit.manifest_target_size = 1;
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(old_file);
    commit.commit(vec![deletion]).await.unwrap();
    let replacement = test_data_file("replacement.parquet", 1);
    commit
        .commit(vec![CommitMessage::new(vec![], 0, vec![replacement])])
        .await
        .unwrap();

    let mut previous = base_manifests(&io, path).await;
    previous.extend(delta_manifests(&io, path).await);
    let [retained, deletion, replacement] = previous.as_slice() else {
        panic!("expected three manifests, got {previous:?}");
    };
    // The retained manifest closes its own group; the other two form the
    // trailing group and merge.
    commit.manifest_target_size = deletion.file_size() + replacement.file_size() + 1;
    assert!(retained.file_size() >= commit.manifest_target_size);
    let unrelated = test_data_file("unrelated.parquet", 1);
    commit
        .commit(vec![CommitMessage::new(vec![], 0, vec![unrelated])])
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
