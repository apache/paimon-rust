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

#[tokio::test]
async fn lookup_compaction_requires_an_enabled_catalog_cache() {
    for options in [
        vec![("deletion-vectors.enabled", "true")],
        vec![("changelog-producer", "lookup")],
        vec![("merge-engine", "first-row")],
    ] {
        let mut configured = options;
        // A table option cannot fabricate a cache on an uncached FileIO.
        configured.push(("local-cache.enabled", "true"));
        let table =
            table_with_cache(pk_schema(&configured), &[("local-cache.enabled", "false")]).await;
        let error = table.new_compact_builder().new_executor().err().unwrap();
        assert!(matches!(error, paimon::Error::ConfigInvalid { .. }));
        assert!(error.to_string().contains("local-cache.enabled=true"));
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            None
        );
    }
    let ordinary = table_with_cache(pk_schema(&[]), &[("local-cache.enabled", "false")]).await;
    assert!(ordinary.new_compact_builder().new_executor().is_ok());
}

async fn incremental_table(options: &[(&str, &str)]) -> Table {
    incremental_table_with_cache(options, &[]).await
}

async fn incremental_table_with_cache(
    options: &[(&str, &str)],
    cache_options: &[(&str, &str)],
) -> Table {
    let mut configured = vec![
        ("bucket", "1"),
        ("num-levels", "6"),
        ("num-sorted-run.compaction-trigger", "5"),
        ("deletion-vectors.enabled", "true"),
        ("changelog-producer", "lookup"),
    ];
    configured.extend_from_slice(options);
    table_with_cache(pk_schema(&configured), cache_options).await
}

async fn compact(
    table: &Table,
    executor: &paimon::table::CompactExecutor,
) -> (CompactPlan, Vec<CompactResult>) {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    let results: Vec<CompactResult> =
        serde_json::from_value(serde_json::to_value(results).unwrap()).unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    (plan, results)
}

#[tokio::test]
async fn incremental_lookup_reuses_local_files_and_keeps_the_large_history_file() {
    for engine in ["deduplicate", "partial-update", "aggregation"] {
        for dv in ["true", "false"] {
            let mut options = vec![
                ("merge-engine", engine),
                ("deletion-vectors.enabled", dv),
                ("read.batch-size", "32"),
            ];
            if engine == "aggregation" {
                options.push(("fields.value.aggregate-function", "sum"));
            }
            let table = incremental_table(&options).await;
            let builder = table.new_compact_builder();
            let executor = builder.new_executor().unwrap();
            write_batch(
                &table,
                &make_batch((0..1000).collect(), (0..1000).collect()),
            )
            .await;
            let (_, first) = compact(&table, &executor).await;
            assert_eq!(first[0].files().len(), 1);
            let anchor = first[0].files()[0].clone();
            assert_eq!(anchor.level, 5);
            for key in [42, 43] {
                write_batch(&table, &make_batch(vec![key], vec![10000 + key])).await;
                let plan = builder.new_planner().unwrap().plan().await.unwrap();
                assert!(plan
                    .tasks()
                    .iter()
                    .flat_map(|task| task.files())
                    .all(|file| file.file_name != anchor.file_name));
                let mut results = Vec::new();
                for task in plan.tasks() {
                    results.push(executor.execute(task).await.unwrap());
                }
                builder
                    .new_commit()
                    .unwrap()
                    .commit_compact(results)
                    .await
                    .unwrap();
                let scan = table
                    .new_read_builder()
                    .new_scan()
                    .with_scan_all_files()
                    .plan()
                    .await
                    .unwrap();
                assert!(scan
                    .splits()
                    .iter()
                    .flat_map(|split| split.data_files())
                    .any(|file| file == &anchor));
            }
            let actual = rows(&table).await;
            assert_eq!(actual.len(), 1000, "{engine}/{dv}");
            for key in [42, 43] {
                assert_eq!(
                    actual[key as usize],
                    (
                        key,
                        if engine == "aggregation" {
                            10000 + 2 * key
                        } else {
                            10000 + key
                        }
                    )
                );
            }
            let metrics = executor.lookup_cache_metrics();
            assert_eq!(
                metrics.file_builds, 1,
                "{engine}/{dv}: only the anchor was built"
            );
            assert!(
                metrics.file_hits >= 1,
                "{engine}/{dv}: subsequent lookups must use local files"
            );
            let earliest = table.snapshot_manager().get_snapshot(2).await.unwrap();
            assert_eq!(
                rows(&table.copy_with_pinned_snapshot(Some(&earliest))).await,
                (0..1000).map(|key| (key, key)).collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn incremental_lookup_pins_historical_vectors_and_reuses_cache_after_vector_changes() {
    let table = incremental_table(&[
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "sum"),
    ])
    .await;
    let builder = table.new_compact_builder();
    let executor = builder.new_executor().unwrap();
    write_batch(
        &table,
        &make_batch((0..1000).collect(), (0..1000).collect()),
    )
    .await;
    let (_, first) = compact(&table, &executor).await;
    let anchor = &first[0].files()[0];
    let start = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    write_batch(&table, &make_batch(vec![42], vec![5])).await;
    let stale = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(stale.tasks()[0]
        .files()
        .iter()
        .all(|file| file.file_name != anchor.file_name));
    let output = executor.execute(&stale.tasks()[0]).await.unwrap();
    super::deletion_vector_tests::delete_positions(&table, &anchor.file_name, &[42]).await;
    assert!(builder
        .new_commit()
        .unwrap()
        .commit_compact(vec![output])
        .await
        .unwrap_err()
        .to_string()
        .contains("deletion vectors changed"));

    // The cached physical value remains 42, but the current snapshot's vector
    // hides it. The retry must emit an INSERT of 5 rather than sum it to 47.
    compact(&table, &executor).await;
    assert_eq!(rows(&table).await[42], (42, 5));
    for (key, value) in [(43, 7), (42, 9)] {
        write_batch(&table, &make_batch(vec![key], vec![value])).await;
        compact(&table, &executor).await;
    }
    let actual = rows(&table).await;
    assert_eq!(actual.len(), 1000);
    assert_eq!(actual[42], (42, 14));
    assert_eq!(actual[43], (43, 50));
    assert_eq!(executor.lookup_cache_metrics().file_builds, 1);
    let end = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    let mut expected = vec![
        ("+I".to_string(), 42, 5),
        ("-U".to_string(), 43, 43),
        ("+U".to_string(), 43, 50),
        ("-U".to_string(), 42, 5),
        ("+U".to_string(), 42, 14),
    ];
    expected.sort();
    assert_eq!(
        super::lookup_changelog_tests::events(&table, start, end).await,
        expected
    );
}

#[tokio::test]
async fn incremental_first_row_does_not_duplicate_a_key_in_an_untouched_higher_level() {
    let table = incremental_table(&[
        ("merge-engine", "first-row"),
        ("deletion-vectors.enabled", "false"),
    ])
    .await;
    let executor = table.new_compact_builder().new_executor().unwrap();
    write_batch(
        &table,
        &make_batch((0..1000).collect(), (0..1000).collect()),
    )
    .await;
    compact(&table, &executor).await;
    write_batch(&table, &make_batch(vec![42], vec![9999])).await;
    let (_, results) = compact(&table, &executor).await;
    assert!(results
        .iter()
        .all(|result| result.files().is_empty() && result.changelog_files().is_empty()));
    let actual = rows(&table).await;
    assert_eq!(actual.len(), 1000);
    assert_eq!(actual[42], (42, 42));
}

#[tokio::test]
async fn incremental_plain_compaction_leaves_history_for_query_merging() {
    for engine in ["deduplicate", "aggregation"] {
        let mut options = vec![
            ("merge-engine", engine),
            ("deletion-vectors.enabled", "false"),
            ("changelog-producer", "none"),
        ];
        if engine == "aggregation" {
            options.push(("fields.value.aggregate-function", "sum"));
        }
        let table = incremental_table(&options).await;
        let executor = table.new_compact_builder().new_executor().unwrap();
        write_batch(
            &table,
            &make_batch((0..1000).collect(), (0..1000).collect()),
        )
        .await;
        let (_, outputs) = compact(&table, &executor).await;
        let anchor = outputs[0].files()[0].file_name.clone();
        write_batch(&table, &make_batch(vec![42], vec![10000])).await;
        let (plan, _) = compact(&table, &executor).await;
        assert!(plan.tasks()[0]
            .files()
            .iter()
            .all(|file| file.file_name != anchor));
        let actual = rows(&table).await;
        assert_eq!(actual.len(), 1000);
        assert_eq!(
            actual[42],
            (
                42,
                if engine == "aggregation" {
                    10042
                } else {
                    10000
                }
            )
        );
        assert_eq!(executor.lookup_cache_metrics().file_builds, 0);
    }
}

fn local_files(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            files.extend(local_files(&entry.path()));
        } else {
            files.push(entry.path());
        }
    }
    files
}

#[tokio::test]
async fn incremental_lookup_reopens_completed_cache_files_and_rebuilds_corrupt_payloads() {
    use std::io::Write;
    for corrupt in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let table = incremental_table_with_cache(
            &[],
            &[("local-cache.dir", directory.path().to_str().unwrap())],
        )
        .await;
        write_batch(
            &table,
            &make_batch((0..1000).collect(), (0..1000).collect()),
        )
        .await;
        {
            let executor = table.new_compact_builder().new_executor().unwrap();
            compact(&table, &executor).await;
            write_batch(&table, &make_batch(vec![42], vec![10042])).await;
            compact(&table, &executor).await;
            assert_eq!(executor.lookup_cache_metrics().file_builds, 1);
        }
        let files = local_files(directory.path());
        assert!(files.iter().any(|file| file
            .components()
            .any(|component| component.as_os_str() == "paimon-local-cache-v2")));
        if corrupt {
            let payload = files
                .iter()
                .find(|file| {
                    let content = std::fs::read(file).unwrap();
                    content
                        .windows(b"/rows".len())
                        .any(|window| window == b"/rows")
                })
                .unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(payload)
                .unwrap()
                .write_all(b"broken")
                .unwrap();
        }
        let executor = table.new_compact_builder().new_executor().unwrap();
        write_batch(&table, &make_batch(vec![43], vec![10043])).await;
        compact(&table, &executor).await;
        assert_eq!(
            executor.lookup_cache_metrics().file_builds,
            u64::from(corrupt)
        );
        assert!(executor.lookup_cache_metrics().file_hits > 0);
        assert_eq!(rows(&table).await[43], (43, 10043));
        assert!(local_files(directory.path()).iter().all(|file| !file
            .components()
            .any(|part| part.as_os_str().to_string_lossy().starts_with(".building-"))));
    }
}
