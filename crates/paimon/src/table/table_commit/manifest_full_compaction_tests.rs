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

#[tokio::test]
async fn full_compaction_drops_resolved_deletes_and_keeps_unrelated_manifests() {
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction";
    setup_dirs(&io, path).await;
    // A one-byte target keeps minor merging idle; a one-byte threshold starts
    // a full compaction as soon as any manifest holds a DELETE.
    let commit = merge_table(
        &io,
        path,
        &[
            ("manifest.target-file-size", "1 b"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    let removed = test_data_file("removed.parquet", 1);
    let mut first = vec![removed.clone()];
    first.extend((0..199).map(|id| test_data_file(&format!("first-{id}.parquet"), 1)));
    let second = (0..200)
        .map(|id| test_data_file(&format!("second-{id}.parquet"), 1))
        .collect();
    commit
        .commit(vec![CommitMessage::new(vec![], 0, first)])
        .await
        .unwrap();
    commit
        .commit(vec![CommitMessage::new(vec![], 0, second)])
        .await
        .unwrap();
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(removed);
    commit.commit(vec![deletion]).await.unwrap();
    let mut previous = base_manifests(&io, path).await;
    previous.extend(delta_manifests(&io, path).await);
    let [first, second, deletion] = previous.as_slice() else {
        panic!("expected three manifests, got {previous:?}");
    };

    let unrelated = test_data_file("unrelated.parquet", 1);
    commit
        .commit(vec![CommitMessage::new(vec![], 0, vec![unrelated])])
        .await
        .unwrap();

    let base = base_manifests(&io, path).await;
    let names = base
        .iter()
        .map(ManifestFileMeta::file_name)
        .collect::<HashSet<_>>();
    assert!(names.contains(second.file_name()));
    assert!(!names.contains(first.file_name()));
    assert!(!names.contains(deletion.file_name()));
    assert!(base
        .iter()
        .all(|manifest| manifest.num_deleted_files() == 0));
    assert_eq!(
        base.iter()
            .map(ManifestFileMeta::num_added_files)
            .sum::<i64>(),
        399
    );
    let live = active_names(&io, path).await;
    assert_eq!(live.len(), 400);
    assert!(!live.contains("removed.parquet"));
}

#[tokio::test]
async fn full_compaction_keeps_a_delete_whose_add_was_pruned() {
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-pruned-add";
    setup_dirs(&io, path).await;
    let writer = partitioned_merge_table(&io, path, &[("manifest.target-file-size", "1 b")]);
    let pt = partition_bytes("a");
    let removed = test_data_file("removed.parquet", 1);
    let mut first = vec![removed.clone()];
    first.extend((0..199).map(|id| test_data_file(&format!("first-{id}.parquet"), 1)));
    writer
        .commit(vec![CommitMessage::new(pt.clone(), 0, first)])
        .await
        .unwrap();
    let mut deletion = CommitMessage::new(pt.clone(), 0, vec![]);
    deletion.deleted_files.push(removed);
    writer.commit(vec![deletion]).await.unwrap();
    // `small` gives the full compaction two manifests to merge; with
    // `deletion` alone it would fall back to minor merging.
    let small = vec![test_data_file("small.parquet", 1)];
    writer
        .commit(vec![CommitMessage::new(pt, 0, small)])
        .await
        .unwrap();
    let mut input = base_manifests(&io, path).await;
    input.extend(delta_manifests(&io, path).await);
    let [first, deletion, small] = input.as_slice() else {
        panic!("expected three manifests, got {input:?}");
    };

    let other_partition = ManifestEntry::new(
        FileKind::Add,
        partition_bytes("b"),
        0,
        1,
        test_data_file("other.parquet", 1),
        2,
    );
    let wrong_stats = writer.compute_partition_stats(&[other_partition]).unwrap();
    let mislabeled = ManifestFileMeta::new(
        first.file_name().to_string(),
        first.file_size(),
        first.num_added_files(),
        first.num_deleted_files(),
        wrong_stats,
        first.schema_id(),
    );
    let merger = partitioned_merge_table(
        &io,
        path,
        &[
            ("manifest.target-file-size", "1 b"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    let mut created = Vec::new();
    let merged = merger
        .merge_manifests(
            vec![mislabeled, deletion.clone(), small.clone()],
            &mut created,
        )
        .await
        .unwrap();

    // deletion was rewritten; its DELETE survives in a new manifest because
    // the manifest holding the matching ADD was skipped.
    assert!(!created.is_empty());
    assert!(merged.iter().all(|m| m.file_name() != deletion.file_name()));
    let rewritten = merged
        .iter()
        .find(|m| m.num_deleted_files() == 1)
        .expect("the unmatched DELETE must be kept");
    assert_eq!(rewritten.num_added_files(), 0);
    let mut entries = Vec::new();
    for manifest in &merged {
        let manifest_path = format!("{path}/manifest/{}", manifest.file_name());
        entries.extend(Manifest::read(&io, &manifest_path).await.unwrap());
    }
    let live = merge_active_entries(entries)
        .into_iter()
        .map(|entry| entry.file().file_name.clone())
        .collect::<HashSet<_>>();
    assert_eq!(live.len(), 200);
    assert!(!live.contains("removed.parquet"));
}

#[tokio::test]
async fn full_compaction_skips_a_manifest_with_an_unknown_field() {
    // Under the default target the new-writer manifest must change. Under a
    // one-byte target it need not, but it is still read: an unpartitioned
    // table reads every manifest once any DELETE exists. The default target
    // runs first so a regression fails on the lost files.
    for target in [None, Some("1 b")] {
        let io = test_file_io();
        let case = if target.is_some() {
            "one-byte-target"
        } else {
            "default-target"
        };
        let path = format!("memory:/manifest-full-compaction-unknown-field-{case}");
        let path = path.as_str();
        setup_dirs(&io, path).await;
        let mut options = vec![("manifest.full-compaction-threshold-size", "1 b")];
        options.extend(target.map(|target| ("manifest.target-file-size", target)));
        let commit = merge_table(&io, path, &options);
        let removed = test_data_file("removed.parquet", 1);
        let mut first = vec![removed.clone()];
        first.extend((0..9).map(|id| test_data_file(&format!("first-{id}.parquet"), 1)));
        commit
            .commit(vec![CommitMessage::new(vec![], 0, first)])
            .await
            .unwrap();
        rewrite_with_unknown_field(&io, path, &delta_manifests(&io, path).await[0]).await;
        let mut deletion = CommitMessage::new(vec![], 0, vec![]);
        deletion.deleted_files.push(removed);
        commit.commit(vec![deletion]).await.unwrap();
        let mut previous = base_manifests(&io, path).await;
        previous.extend(delta_manifests(&io, path).await);
        assert_eq!(previous.len(), 2, "{case}");

        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("unrelated.parquet", 1)],
            )])
            .await
            .unwrap();

        // Neither manifest was rewritten. A must-change manifest written by a
        // newer writer would be rewritten without its entries, and any rewrite
        // would drop its unknown field.
        let live = active_names(&io, path).await;
        assert_eq!(live.len(), 10, "{case}");
        assert!(!live.contains("removed.parquet"), "{case}");
        assert_eq!(base_manifests(&io, path).await, previous, "{case}");
    }
}

#[tokio::test]
async fn full_compaction_keeps_manifests_that_re_add_a_deleted_file() {
    // An ordered reader keeps x live after [ADD x, DELETE x, ADD x]; dropping
    // every ADD of a deleted file would remove it.
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-re-add";
    setup_dirs(&io, path).await;
    // The default threshold keeps these commits from compacting the first
    // ADD and DELETE away.
    let writer = merge_table(&io, path, &[("manifest.merge-min-count", "1000")]);
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    for message in [
        append_message("x.parquet"),
        deletion,
        append_message("x.parquet"),
    ] {
        writer.commit(vec![message]).await.unwrap();
    }
    let mut previous = base_manifests(&io, path).await;
    previous.extend(delta_manifests(&io, path).await);
    assert_eq!(previous.len(), 3);

    let merger = merge_table(
        &io,
        path,
        &[
            ("manifest.merge-min-count", "1000"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    merger
        .commit(vec![append_message("y.parquet")])
        .await
        .unwrap();
    assert_eq!(base_manifests(&io, path).await, previous);
    assert_eq!(
        active_names(&io, path).await,
        HashSet::from(["x.parquet".to_string(), "y.parquet".to_string()])
    );
}

#[tokio::test]
async fn full_compaction_starts_when_must_change_size_equals_the_threshold() {
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-threshold";
    setup_dirs(&io, path).await;
    // A one-byte target keeps minor merging idle, and only the manifest
    // holding the DELETE must change.
    let writer = merge_table(&io, path, &[("manifest.target-file-size", "1 b")]);
    let removed = test_data_file("removed.parquet", 1);
    let kept = test_data_file("kept.parquet", 1);
    writer
        .commit(vec![CommitMessage::new(
            vec![],
            0,
            vec![removed.clone(), kept],
        )])
        .await
        .unwrap();
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(removed);
    writer.commit(vec![deletion]).await.unwrap();
    let mut input = base_manifests(&io, path).await;
    input.extend(delta_manifests(&io, path).await);
    let [_, deletion] = input.as_slice() else {
        panic!("expected two manifests, got {input:?}");
    };
    let must_change = input
        .iter()
        .filter(|manifest| manifest.num_deleted_files() > 0)
        .map(ManifestFileMeta::file_size)
        .sum::<i64>();
    assert_eq!(must_change, deletion.file_size());

    let threshold = format!("{must_change} b");
    let merger = merge_table(
        &io,
        path,
        &[
            ("manifest.target-file-size", "1 b"),
            (
                "manifest.full-compaction-threshold-size",
                threshold.as_str(),
            ),
        ],
    );
    let mut created = Vec::new();
    let merged = merger
        .merge_manifests(input.clone(), &mut created)
        .await
        .unwrap();
    assert!(!created.is_empty());
    assert!(merged.iter().all(|m| m.file_name() != deletion.file_name()));
    assert_eq!(merged.len(), 1);
    assert_eq!(
        (merged[0].num_added_files(), merged[0].num_deleted_files()),
        (1, 0)
    );
}

#[tokio::test]
async fn full_compaction_without_deletes_merges_below_min_count() {
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-no-deletes";
    setup_dirs(&io, path).await;
    // Under the default target every manifest is small, so each commit from
    // the third on rewrites the base and delta into one manifest.
    let commit = merge_table(
        &io,
        path,
        &[
            ("manifest.merge-min-count", "1000"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    for name in ["first", "second", "third", "fourth"] {
        commit.commit(vec![append_message(name)]).await.unwrap();
    }

    let base = base_manifests(&io, path).await;
    assert_eq!(base.len(), 1);
    assert_eq!(
        (base[0].num_added_files(), base[0].num_deleted_files()),
        (3, 0)
    );
    assert_eq!(
        active_names(&io, path).await,
        ["first", "second", "third", "fourth"]
            .map(String::from)
            .into_iter()
            .collect::<HashSet<_>>()
    );
}

#[tokio::test]
async fn full_compaction_of_a_single_manifest_falls_back_to_minor_merging() {
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-single";
    setup_dirs(&io, path).await;
    let writer = merge_table(&io, path, &[]);
    writer
        .commit(vec![append_message("x.parquet")])
        .await
        .unwrap();
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    writer.commit(vec![deletion]).await.unwrap();
    let input = delta_manifests(&io, path).await;
    assert_eq!(input.len(), 1);
    assert_eq!(input[0].num_deleted_files(), 1);

    let merger = merge_table(
        &io,
        path,
        &[("manifest.full-compaction-threshold-size", "1 b")],
    );
    let mut created = Vec::new();
    let merged = merger
        .merge_manifests(input.clone(), &mut created)
        .await
        .unwrap();
    assert_eq!(merged, input);
    assert!(created.is_empty());
}

#[tokio::test]
async fn full_compaction_keeps_manifests_whose_delete_count_is_understated() {
    // `read_deleted_entries` skips a manifest listed without DELETEs, so
    // rewriting it would drop the DELETE of x and bring x back.
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-understated-deletes";
    setup_dirs(&io, path).await;
    let writer = merge_table(&io, path, &[("manifest.merge-min-count", "1000")]);
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    for message in [
        append_message("x.parquet"),
        deletion,
        append_message("y.parquet"),
    ] {
        writer.commit(vec![message]).await.unwrap();
    }
    let mut input = base_manifests(&io, path).await;
    input.extend(delta_manifests(&io, path).await);
    let [_, deletion, _] = input.as_slice() else {
        panic!("expected three manifests, got {input:?}");
    };
    assert_eq!(deletion.num_deleted_files(), 1);
    input[1] = ManifestFileMeta::new(
        deletion.file_name().to_string(),
        deletion.file_size(),
        deletion.num_added_files(),
        0,
        deletion.partition_stats().clone(),
        deletion.schema_id(),
    );

    let merger = merge_table(
        &io,
        path,
        &[
            ("manifest.merge-min-count", "1000"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    let mut created = Vec::new();
    let merged = merger
        .merge_manifests(input.clone(), &mut created)
        .await
        .unwrap();
    let mut entries = Vec::new();
    for manifest in &merged {
        let manifest_path = format!("{path}/manifest/{}", manifest.file_name());
        entries.extend(Manifest::read(&io, &manifest_path).await.unwrap());
    }
    let live = merge_active_entries(entries)
        .into_iter()
        .map(|entry| entry.file().file_name.clone())
        .collect::<HashSet<_>>();
    assert_eq!(live, HashSet::from(["y.parquet".to_string()]));
    assert_eq!(merged, input);
    assert!(created.is_empty());
}
