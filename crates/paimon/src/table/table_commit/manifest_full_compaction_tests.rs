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
async fn full_compaction_drops_deletes_whose_add_is_gone() {
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-lone-delete";
    setup_dirs(&io, path).await;
    // The third commit merges [ADD x, DELETE x], which leaves only the DELETE.
    let writer = merge_table(&io, path, &[("manifest.merge-min-count", "2")]);
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    for message in [
        append_message("x.parquet"),
        deletion,
        append_message("y.parquet"),
    ] {
        writer.commit(vec![message]).await.unwrap();
    }
    let lone_delete = base_manifests(&io, path).await.remove(0);
    assert_eq!(lone_delete.num_added_files(), 0);
    assert_eq!(lone_delete.num_deleted_files(), 1);

    let merger = merge_table(
        &io,
        path,
        &[("manifest.full-compaction-threshold-size", "1 b")],
    );
    let before = live_files_by_reader(&merger, &io, path).await;
    merger
        .commit(vec![append_message("unrelated.parquet")])
        .await
        .unwrap();

    assert_live_files_plus_unrelated(&merger, &io, path, before).await;
    let base = base_manifests(&io, path).await;
    assert!(base
        .iter()
        .all(|m| m.file_name() != lone_delete.file_name()));
    assert_no_delete_entries(&io, path, &base).await;
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
async fn full_compaction_drops_a_deleted_file_that_was_added_again() {
    // After [ADD x, DELETE x, ADD x] scans treat x as deleted and the ordered
    // reader lists it. Full compaction drops every ADD of x, so both readers
    // then list what the scan listed.
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-re-add";
    setup_dirs(&io, path).await;
    // The default threshold and a min-count of 1000 keep these commits from
    // merging.
    let writer = merge_table(&io, path, &[("manifest.merge-min-count", "1000")]);
    let first = ["x.parquet", "w.parquet"].map(|name| test_data_file(name, 10));
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files.push(test_data_file("x.parquet", 10));
    for message in [
        CommitMessage::new(vec![], 0, first.to_vec()),
        deletion,
        append_message("x.parquet"),
    ] {
        writer.commit(vec![message]).await.unwrap();
    }

    let merger = merge_table(
        &io,
        path,
        &[
            ("manifest.merge-min-count", "1000"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    let [scanned, listed] = live_files_by_reader(&merger, &io, path).await;
    let x = ("x.parquet".to_string(), 0);
    assert!(!scanned.contains(&x) && listed.contains(&x));
    merger
        .commit(vec![append_message("unrelated.parquet")])
        .await
        .unwrap();

    assert_live_files_plus_unrelated(&merger, &io, path, [scanned.clone(), scanned]).await;
    assert_no_delete_entries(&io, path, &base_manifests(&io, path).await).await;
}

/// Assert that none of `manifests` holds a DELETE entry.
async fn assert_no_delete_entries(io: &FileIO, path: &str, manifests: &[ManifestFileMeta]) {
    for manifest in manifests {
        let manifest_path = format!("{path}/manifest/{}", manifest.file_name());
        let entries = Manifest::read(io, &manifest_path).await.unwrap();
        assert!(
            entries.iter().all(|entry| *entry.kind() == FileKind::Add),
            "{}",
            manifest.file_name()
        );
    }
}

#[tokio::test]
async fn full_compaction_does_not_read_manifests_outside_deleted_partitions() {
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-partition-pruning";
    setup_dirs(&io, path).await;
    // A one-byte target keeps minor merging idle, and only the manifest
    // holding the DELETE must change.
    let writer = partitioned_merge_table(&io, path, &[("manifest.target-file-size", "1 b")]);
    let (a, b) = (partition_bytes("a"), partition_bytes("b"));
    let x = test_data_file("x.parquet", 1);
    let mut deletion = CommitMessage::new(a.clone(), 0, vec![]);
    deletion.deleted_files.push(x.clone());
    for message in [
        CommitMessage::new(b, 0, vec![test_data_file("b.parquet", 1)]),
        CommitMessage::new(a, 0, vec![x, test_data_file("z.parquet", 1)]),
        deletion,
    ] {
        writer.commit(vec![message]).await.unwrap();
    }
    let mut input = base_manifests(&io, path).await;
    input.extend(delta_manifests(&io, path).await);
    let in_b = input[0].clone();
    assert_eq!((in_b.num_added_files(), in_b.num_deleted_files()), (1, 0));
    // Partition statistics show that in_b holds no file of partition a.
    // Deleting it makes any read of it fail.
    io.delete_file(&format!("{path}/manifest/{}", in_b.file_name()))
        .await
        .unwrap();

    let merger = partitioned_merge_table(
        &io,
        path,
        &[
            ("manifest.target-file-size", "1 b"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    let mut created = Vec::new();
    let merged = merger.merge_manifests(input, &mut created).await.unwrap();
    assert!(!created.is_empty());
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].file_name(), in_b.file_name());
    assert_eq!(
        (merged[1].num_added_files(), merged[1].num_deleted_files()),
        (1, 0)
    );
    assert_no_delete_entries(&io, path, &merged[1..]).await;
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

/// A write-only table partitioned by a DOUBLE column.
fn double_partitioned_table(io: &FileIO, path: &str, options: &[(&str, &str)]) -> TableCommit {
    let mut builder = crate::spec::Schema::builder()
        .column("pt", DataType::Double(crate::spec::DoubleType::new()))
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

fn double_partition(value: f64) -> Vec<u8> {
    let mut builder = BinaryRowBuilder::new(1);
    let partition_type = DataType::Double(crate::spec::DoubleType::new());
    builder.write_datum(0, &Datum::Double(value), &partition_type);
    builder.build_serialized()
}

fn deletion_of(partition: &[u8], name: &str) -> CommitMessage {
    let mut deletion = CommitMessage::new(partition.to_vec(), 0, vec![]);
    deletion.deleted_files.push(test_data_file(name, 1));
    deletion
}

#[tokio::test]
async fn full_compaction_reads_every_manifest_of_a_float_partitioned_table() {
    // Rust before #1190 wrote the statistics of [3.0, NaN, 5.0] as min = max =
    // 5.0. The test fakes those statistics. Pruning on them skips the manifest
    // that adds x, so the compaction would drop the DELETE of x and bring x
    // back.
    let io = test_file_io();
    let path = "memory:/manifest-full-compaction-float-partition";
    setup_dirs(&io, path).await;
    // A one-byte target keeps minor merging idle, and only the manifest
    // holding the DELETE must change.
    let writer = double_partitioned_table(&io, path, &[("manifest.target-file-size", "1 b")]);
    let (three, five) = (double_partition(3.0), double_partition(5.0));
    for messages in [
        vec![
            CommitMessage::new(three.clone(), 0, vec![test_data_file("x.parquet", 1)]),
            CommitMessage::new(five.clone(), 0, vec![test_data_file("y.parquet", 1)]),
        ],
        vec![deletion_of(&three, "x.parquet")],
        // A second manifest in the deleted partition: without it, a compaction
        // that skips the first manifest has one manifest to merge and falls
        // back to a minor merge.
        vec![CommitMessage::new(
            three,
            0,
            vec![test_data_file("z.parquet", 1)],
        )],
    ] {
        writer.commit(messages).await.unwrap();
    }
    let mut input = base_manifests(&io, path).await;
    input.extend(delta_manifests(&io, path).await);
    assert_eq!(input.len(), 3);
    let first = &input[0];
    assert_eq!((first.num_added_files(), first.num_deleted_files()), (2, 0));
    let in_five = ManifestEntry::new(FileKind::Add, five, 0, 1, test_data_file("y.parquet", 1), 2);
    input[0] = ManifestFileMeta::new(
        first.file_name().to_string(),
        first.file_size(),
        first.num_added_files(),
        first.num_deleted_files(),
        writer.compute_partition_stats(&[in_five]).unwrap(),
        first.schema_id(),
    );

    let merger = double_partitioned_table(
        &io,
        path,
        &[
            ("manifest.target-file-size", "1 b"),
            ("manifest.full-compaction-threshold-size", "1 b"),
        ],
    );
    let mut created = Vec::new();
    let merged = merger.merge_manifests(input, &mut created).await.unwrap();
    let mut entries = Vec::new();
    for manifest in &merged {
        let manifest_path = format!("{path}/manifest/{}", manifest.file_name());
        entries.extend(Manifest::read(&io, &manifest_path).await.unwrap());
    }
    let deleted = entries
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete)
        .map(ManifestEntry::identifier)
        .collect::<HashSet<_>>();
    let scanned = entries
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Add && !deleted.contains(&entry.identifier()))
        .map(|entry| entry.file().file_name.clone())
        .collect::<HashSet<_>>();
    let listed = merge_active_entries(entries)
        .into_iter()
        .map(|entry| entry.file().file_name.clone())
        .collect::<HashSet<_>>();
    let expected = HashSet::from(["y.parquet".to_string(), "z.parquet".to_string()]);
    assert_eq!(scanned, expected, "scan reader");
    assert_eq!(listed, expected, "ordered reader");
    // The full compaction ran: it dropped every DELETE.
    assert_no_delete_entries(&io, path, &merged).await;
}

#[tokio::test]
async fn full_compaction_keeps_a_file_in_a_nan_partition_deleted() {
    // With x deleted in partition NaN and z in 1.0, pruning bounds depended on
    // the order of a hash set, and about half the orders skipped the manifest
    // that adds x. Each run builds a new set.
    for run in 0..8 {
        let io = test_file_io();
        let path = format!("memory:/manifest-full-compaction-nan-partition-{run}");
        let path = path.as_str();
        setup_dirs(&io, path).await;
        let writer = double_partitioned_table(&io, path, &[("manifest.target-file-size", "1 b")]);
        let (nan, one) = (double_partition(f64::NAN), double_partition(1.0));
        for messages in [
            vec![
                CommitMessage::new(nan.clone(), 0, vec![test_data_file("x.parquet", 1)]),
                CommitMessage::new(
                    double_partition(5.0),
                    0,
                    vec![test_data_file("y.parquet", 1)],
                ),
            ],
            vec![CommitMessage::new(
                one.clone(),
                0,
                vec![test_data_file("z.parquet", 1)],
            )],
            vec![
                deletion_of(&nan, "x.parquet"),
                deletion_of(&one, "z.parquet"),
            ],
        ] {
            writer.commit(messages).await.unwrap();
        }

        let merger = double_partitioned_table(
            &io,
            path,
            &[
                ("manifest.target-file-size", "1 b"),
                ("manifest.full-compaction-threshold-size", "1 b"),
            ],
        );
        let unrelated = vec![test_data_file("unrelated.parquet", 1)];
        merger
            .commit(vec![CommitMessage::new(one, 0, unrelated)])
            .await
            .unwrap();

        let expected = vec![
            ("unrelated.parquet".to_string(), 0),
            ("y.parquet".to_string(), 0),
        ];
        let actual = live_files_by_reader(&merger, &io, path).await;
        assert_eq!(actual, [expected.clone(), expected], "run {run}");
        assert_no_delete_entries(&io, path, &base_manifests(&io, path).await).await;
    }
}
