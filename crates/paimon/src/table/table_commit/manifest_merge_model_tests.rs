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

fn file_at_level(name: &str, level: i32) -> DataFileMeta {
    let mut file = test_data_file(name, 1);
    file.level = level;
    file
}

fn message_for<'a>(
    changes: &'a mut std::collections::BTreeMap<&'static str, CommitMessage>,
    pt: &'static str,
) -> &'a mut CommitMessage {
    changes
        .entry(pt)
        .or_insert_with(|| CommitMessage::new(partition_bytes(pt), 0, vec![]))
}

/// (partition, file name, level) of a file that the model holds live.
type ModelFile = (&'static str, String, i32);

/// Xorshift, so that each seed replays the same commits.
struct ModelRandom(u64);

impl ModelRandom {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

/// The messages of one random commit, applied to `live` as well: maybe a
/// DELETE of a live file, maybe an upgrade of one to the next level, then one
/// to five new files in one partition.
fn random_commit(
    step: usize,
    random: &mut ModelRandom,
    live: &mut Vec<ModelFile>,
) -> Vec<CommitMessage> {
    let mut changes = std::collections::BTreeMap::new();
    if live.len() > 1 && random.below(3) == 0 {
        let (pt, name, level) = live.swap_remove(random.below(live.len()));
        message_for(&mut changes, pt)
            .deleted_files
            .push(file_at_level(&name, level));
    }
    if !live.is_empty() && random.below(5) == 0 {
        let position = random.below(live.len());
        let (pt, name, level) = live[position].clone();
        let message = message_for(&mut changes, pt);
        message.deleted_files.push(file_at_level(&name, level));
        message.new_files.push(file_at_level(&name, level + 1));
        live[position].2 = level + 1;
    }
    let pt = ["a", "b", "c"][random.below(3)];
    for index in 0..=random.below(4) {
        let name = format!("f-{step}-{index}.parquet");
        message_for(&mut changes, pt)
            .new_files
            .push(file_at_level(&name, 0));
        live.push((pt, name, 0));
    }
    changes.into_values().collect()
}

/// Assert that the scan and the ordered reader both resolve the latest
/// snapshot to the live files of the model.
async fn assert_live_files_match(
    commit: &TableCommit,
    io: &FileIO,
    path: &str,
    live: &[ModelFile],
    context: &str,
) {
    let mut expected = live
        .iter()
        .map(|(_, name, level)| (name.clone(), *level))
        .collect::<Vec<_>>();
    expected.sort();
    let readers = live_files_by_reader(commit, io, path).await;
    for (actual, reader) in readers.into_iter().zip(["scan", "ordered"]) {
        assert_eq!(actual, expected, "{context}, {reader} reader");
    }
}

/// Whether a merged manifest of `base`, one not in `seen_deltas`, holds a
/// DELETE whose ADD is in another manifest of `base`: the #899 case.
async fn merged_delete_with_add_elsewhere(
    io: &FileIO,
    path: &str,
    base: &[ManifestFileMeta],
    seen_deltas: &HashSet<String>,
) -> bool {
    let mut adds = std::collections::HashMap::new();
    let mut merged_deletes = Vec::new();
    for manifest in base {
        let manifest_path = format!("{path}/manifest/{}", manifest.file_name());
        let merged = !seen_deltas.contains(manifest.file_name());
        for entry in Manifest::read(io, &manifest_path).await.unwrap() {
            match entry.kind() {
                FileKind::Add => {
                    adds.insert(entry.identifier(), manifest.file_name());
                }
                FileKind::Delete if merged => {
                    merged_deletes.push((entry.identifier(), manifest.file_name()));
                }
                FileKind::Delete => {}
            }
        }
    }
    merged_deletes
        .iter()
        .any(|(identifier, name)| adds.get(identifier).is_some_and(|add| add != name))
}

fn deleted_file_count(manifests: &[ManifestFileMeta]) -> i64 {
    manifests
        .iter()
        .map(ManifestFileMeta::num_deleted_files)
        .sum()
}

/// What one seed's commits exercised.
#[derive(Default)]
struct ModelCoverage {
    merged: bool,
    cross_group_delete: bool,
    full_compactions: usize,
}

/// Run one seed's random commits and check both readers against the model
/// after each commit.
async fn run_model_commits(seed: u64) -> ModelCoverage {
    let io = test_file_io();
    let path = format!("memory:/manifest-merge-model-{seed}");
    let path = path.as_str();
    setup_dirs(&io, path).await;
    // A manifest of a few entries is about 2.4 kb. A merged manifest that
    // reaches the 3 kb target closes a group by itself, so later groups hold
    // DELETEs whose ADD is outside them. The 5 kb threshold makes full
    // compaction run.
    let commit = partitioned_merge_table(
        &io,
        path,
        &[
            ("manifest.target-file-size", "3 kb"),
            ("manifest.merge-min-count", "3"),
            ("manifest.full-compaction-threshold-size", "5 kb"),
        ],
    );
    let mut random = ModelRandom(seed);
    let mut live = Vec::new();
    let mut seen_deltas = HashSet::new();
    let mut coverage = ModelCoverage::default();
    // DELETE entries in the manifests the next commit merges.
    let mut carried_deletes = 0;
    for step in 0..150 {
        let messages = random_commit(step, &mut random, &mut live);
        commit.commit(messages).await.unwrap();
        let context = format!("seed {seed}, step {step}");
        assert_live_files_match(&commit, &io, path, &live, &context).await;
        let base = base_manifests(&io, path).await;
        let deltas = delta_manifests(&io, path).await;
        // A minor merge keeps every DELETE, and the model never deletes a
        // file twice, so only a full compaction lowers the count.
        if deleted_file_count(&base) < carried_deletes {
            coverage.full_compactions += 1;
        }
        carried_deletes = deleted_file_count(&base) + deleted_file_count(&deltas);
        seen_deltas.extend(deltas.iter().map(|m| m.file_name().to_string()));
        coverage.merged |= base.iter().any(|m| !seen_deltas.contains(m.file_name()));
        if !coverage.cross_group_delete {
            coverage.cross_group_delete =
                merged_delete_with_add_elsewhere(&io, path, &base, &seen_deltas).await;
        }
    }
    assert!(base_manifests(&io, path).await.len() < 50, "seed {seed}");
    coverage
}

/// Seeded random commits checked against a model of the live files. Small
/// targets and thresholds make commits merge and full-compact often.
#[tokio::test]
async fn manifest_merges_preserve_live_files_across_random_commits() {
    for seed in [1u64, 7, 42] {
        let coverage = run_model_commits(seed).await;
        assert!(coverage.merged, "seed {seed} never merged manifests");
        assert!(
            coverage.cross_group_delete,
            "seed {seed} never merged a DELETE whose ADD is in another manifest (the #899 case); retune the parameters"
        );
        assert!(
            coverage.full_compactions > 0,
            "seed {seed} never ran a full compaction that dropped DELETEs; retune the parameters"
        );
    }
}
