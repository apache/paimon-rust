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
use paimon::table::{MergeAssignment, MergeSource, TableCommit, WhenMatched, WhenNotMatched};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn historical_append_writers_do_not_require_query_read_grants() {
    for empty in [false, true] {
        for policy in ["denied", "filter", "mask"] {
            let g = written(
                "historical_append_target",
                |options| {
                    people(
                        &[
                            options,
                            &[
                                ("data-evolution.enabled", "true"),
                                ("row-tracking.enabled", "true"),
                            ],
                        ]
                        .concat(),
                    )
                },
                vec![people_rows(1..=2), people_rows(3..=3)],
            )
            .await;
            let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
            let target = g
                .table
                .copy_with_pinned_snapshot((!empty).then_some(&snapshot));
            match policy {
                "denied" => g.ctx.server.set_column_auth(
                    "default",
                    "historical_append_target",
                    vec!["id".into()],
                ),
                "filter" => g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]),
                "mask" => g.set_rules(&[], &[("name", upper(1, "name"))]),
                _ => unreachable!(),
            }
            let builder = target.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(&people_batch(&[(4, "new")]))
                .await
                .unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            assert!(!messages.is_empty());
            assert!(messages
                .iter()
                .flat_map(|message| &message.new_files)
                .all(|file| file.schema_id == target.schema().id() && file.first_row_id.is_none()));
            builder.try_new_commit().unwrap();
            assert!(g
                .ctx
                .server
                .auth_selects("default", "historical_append_target")
                .is_empty());

            // The mock REST endpoint serves SELECT authorization, not commit
            // publication. Publish through its backing filesystem fixture.
            let mut options = Options::new();
            options.set(
                CatalogOptions::WAREHOUSE,
                g._tmp.path().to_string_lossy().replace('\\', "/"),
            );
            let publisher = FileSystemCatalog::new(options)
                .unwrap()
                .get_table(&g.identifier)
                .await
                .unwrap();
            TableCommit::new(publisher.clone(), "historical-append".into())
                .commit(messages)
                .await
                .unwrap();
            assert_eq!(
                ids(&read_all(&publisher.new_read_builder()).await.unwrap()),
                [1, 2, 3, 4]
            );

            // A supplied schema cannot obtain publication authority merely by
            // selecting a historical view of the same physical table.
            let assembled = Table::new(
                target.file_io().clone(),
                target.identifier().clone(),
                target.location().into(),
                target.schema().clone(),
                None,
            )
            .copy_with_pinned_snapshot((!empty).then_some(&snapshot));
            assert_refused(assembled.new_write_builder().new_write().err().unwrap());
            assert_refused(
                assembled
                    .new_write_builder()
                    .try_new_commit()
                    .err()
                    .unwrap(),
            );
            assert_refused(
                TableCommit::new(assembled, "assembled".into())
                    .commit(Vec::new())
                    .await
                    .unwrap_err(),
            );
            assert!(g
                .ctx
                .server
                .auth_selects("default", "historical_append_target")
                .is_empty());
        }
    }
}

#[tokio::test]
async fn historical_insert_mutations_require_whole_target_grants_before_staging() {
    for operation in ["upsert", "merge"] {
        for empty in [false, true] {
            for policy in ["whole", "denied", "filter", "mask"] {
                let g = written(
                    "historical_insert_target",
                    |options| {
                        people(
                            &[
                                options,
                                &[
                                    ("data-evolution.enabled", "true"),
                                    ("row-tracking.enabled", "true"),
                                    ("deletion-vectors.enabled", "true"),
                                ],
                            ]
                            .concat(),
                        )
                    },
                    vec![people_rows(1..=2), people_rows(3..=3)],
                )
                .await;
                let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
                let target = g
                    .table
                    .copy_with_pinned_snapshot((!empty).then_some(&snapshot));
                match policy {
                    "denied" => g.ctx.server.set_column_auth(
                        "default",
                        "historical_insert_target",
                        vec!["id".into()],
                    ),
                    "filter" => g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]),
                    "mask" => g.set_rules(&[], &[("name", upper(1, "name"))]),
                    _ => g.set_rules(&[], &[]),
                }
                let before = target
                    .file_io()
                    .list_status_recursive(target.location())
                    .await
                    .unwrap()
                    .len();
                let calls = Arc::new(AtomicUsize::new(0));
                let called = calls.clone();
                let assignment = MergeAssignment::Value(paimon::table::UpdateAssignment::Function(
                    Arc::new(move |batches| {
                        called.fetch_add(1, Ordering::SeqCst);
                        Ok(batches
                            .iter()
                            .map(|batch| batch.column_by_name("s.name").unwrap().clone())
                            .collect())
                    }),
                ));
                let mut update = target.new_write_builder().new_update().unwrap();
                update.with_update_type(vec!["name".into()]).unwrap();
                let source = people_batch(&[(1, "changed"), (4, "new")]);
                let result = if operation == "upsert" {
                    update
                        .upsert_by_arrow_with_key(vec![source], vec!["id".into()])
                        .await
                } else {
                    update
                        .merge_into(
                            MergeSource::Batches(vec![source]),
                            vec![("id".into(), "id".into())],
                            vec![WhenMatched {
                                condition: None,
                                delete: false,
                                assignments: vec![("name".into(), assignment.clone())],
                            }],
                            vec![WhenNotMatched {
                                condition: None,
                                assignments: vec![
                                    ("id".into(), MergeAssignment::SourceColumn("id".into())),
                                    ("name".into(), assignment),
                                ],
                            }],
                        )
                        .await
                };
                if policy == "whole" {
                    let messages = result.unwrap();
                    assert!(!messages.is_empty());
                    if operation == "merge" {
                        assert!(calls.load(Ordering::SeqCst) > 0);
                    }
                    assert!(messages
                        .iter()
                        .any(|message| message.check_from_snapshot.is_none()));
                    // This fixture's REST server supplies grants, while its
                    // filesystem catalog owns snapshot publication.
                    let mut options = Options::new();
                    options.set(
                        CatalogOptions::WAREHOUSE,
                        g._tmp.path().to_string_lossy().replace('\\', "/"),
                    );
                    let publisher = FileSystemCatalog::new(options)
                        .unwrap()
                        .get_table(&g.identifier)
                        .await
                        .unwrap();
                    TableCommit::new(publisher, "historical-inserts".into())
                        .commit(messages)
                        .await
                        .unwrap();
                    let mut expected = vec![
                        (1, Some("changed".into())),
                        (2, Some("bob".into())),
                        (3, Some("carol".into())),
                        (4, Some("new".into())),
                    ];
                    if empty {
                        expected.push((1, Some("alice".into())));
                        expected.sort();
                    }
                    assert_eq!(
                        id_names(&read_all(&g.table.new_read_builder()).await.unwrap()),
                        expected
                    );
                } else {
                    let error = result.unwrap_err();
                    if policy == "denied" {
                        assert_forbidden(error);
                    } else {
                        assert!(error.to_string().contains("unrestricted"), "{error}");
                    }
                    assert_eq!(calls.load(Ordering::SeqCst), 0);
                    assert_eq!(
                        target
                            .file_io()
                            .list_status_recursive(target.location())
                            .await
                            .unwrap()
                            .len(),
                        before
                    );
                    assert_eq!(
                        target
                            .snapshot_manager()
                            .get_latest_snapshot_id()
                            .await
                            .unwrap(),
                        Some(2)
                    );
                }
                let selects = g
                    .ctx
                    .server
                    .auth_selects("default", "historical_insert_target");
                assert!(!selects.is_empty());
                assert!(selects.iter().all(Option::is_none));
            }
        }
    }
}

fn target() -> Table {
    Table::new(
        paimon::io::FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "merge_target"),
        "memory:/merge_target".into(),
        paimon::spec::TableSchema::new(
            0,
            &people(&[
                ("data-evolution.enabled", "true"),
                ("row-tracking.enabled", "true"),
                ("deletion-vectors.enabled", "true"),
            ]),
        ),
        None,
    )
}

async fn merge(target: &Table, source: &Table, delete: bool) -> paimon::Result<()> {
    let messages = target
        .new_write_builder()
        .new_update()?
        .merge_into(
            MergeSource::Table(Arc::new(source.clone())),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete,
                assignments: if delete {
                    Vec::new()
                } else {
                    vec![("name".into(), MergeAssignment::SourceColumn("name".into()))]
                },
            }],
            if delete {
                Vec::new()
            } else {
                vec![WhenNotMatched {
                    condition: None,
                    assignments: vec![
                        ("id".into(), MergeAssignment::SourceColumn("id".into())),
                        ("name".into(), MergeAssignment::SourceColumn("name".into())),
                    ],
                }]
            },
        )
        .await?;
    TableCommit::new(target.clone(), "authorized-merge".into())
        .commit(messages)
        .await?;
    Ok(())
}

#[tokio::test]
async fn unrestricted_source_supports_merge_and_keeps_metadata_scans_closed() {
    let g = written("merge_source", people, vec![people_rows(1..=2)]).await;
    g.set_rules(&[], &[]);
    let target = target();
    write_batch(&target, people_batch(&[(1, "old"), (3, "keep")]), "seed").await;
    merge(&target, &g.table, false).await.unwrap();
    assert_eq!(
        id_names(&read_all(&target.new_read_builder()).await.unwrap()),
        [
            (1, Some("alice".into())),
            (2, Some("bob".into())),
            (3, Some("keep".into())),
        ]
    );
    assert_eq!(g.ctx.server.auth_selects("default", "merge_source"), [None]);
    // An unrestricted data-read grant must not expose a public metadata bypass.
    assert!(g
        .table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .is_err());
}

#[tokio::test]
async fn unrestricted_source_authorizes_empty_and_historical_tables() {
    let g = written(
        "merge_history",
        people,
        vec![people_rows(1..=1), people_rows(2..=2)],
    )
    .await;
    g.set_rules(&[], &[]);
    let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
    for (index, view) in [
        g.table.copy_with_pinned_snapshot(None),
        g.table.copy_with_pinned_snapshot(Some(&snapshot)),
    ]
    .into_iter()
    .enumerate()
    {
        let target = target();
        write_batch(&target, people_batch(&[(1, "old")]), "seed").await;
        merge(&target, &view, false).await.unwrap();
        let rows = id_names(&read_all(&target.new_read_builder()).await.unwrap());
        assert_eq!(
            rows,
            [(1, Some(if index == 0 { "old" } else { "alice" }.into()))]
        );
    }
    let empty = written("merge_empty", people, vec![]).await;
    empty.set_rules(&[], &[]);
    let target = target();
    write_batch(&target, people_batch(&[(1, "old")]), "seed").await;
    merge(&target, &empty.table, true).await.unwrap();
    assert_eq!(
        ids(&read_all(&target.new_read_builder()).await.unwrap()),
        [1]
    );
    assert_eq!(
        empty.ctx.server.auth_selects("default", "merge_empty"),
        [None]
    );
}

#[tokio::test]
async fn merge_rejects_restricted_sources_even_for_unused_fields_or_empty_views() {
    for empty in [false, true] {
        let source = if empty {
            // The empty branch here is a genuinely empty current table, not
            // a historical handle whose authorization is separately refused.
            written("merge_rules_empty", people, vec![]).await
        } else {
            written("merge_rules_full", people, vec![people_rows(1..=2)]).await
        };
        let target = target();
        write_batch(&target, people_batch(&[(1, "old")]), "seed").await;
        for mask in [false, true] {
            if mask {
                source.set_rules(&[], &[("name", upper(1, "name"))]);
            } else {
                source.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
            }
            // DELETE matches only `id`: an unused column's mask still makes the
            // source unsuitable for Python's existing plan_all_files contract.
            let error = merge(&target, &source.table, true).await.unwrap_err();
            assert!(error.to_string().contains("unrestricted"), "{error}");
            assert_eq!(
                ids(&read_all(&target.new_read_builder()).await.unwrap()),
                [1]
            );
            assert_eq!(
                target
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                Some(1)
            );
        }
        assert!(source
            .ctx
            .server
            .auth_selects("default", source.identifier.object())
            .iter()
            .all(Option::is_none));
    }
}

#[tokio::test]
async fn merge_authorizes_all_source_columns_and_refreshes_each_operation() {
    let g = written("merge_columns", people, vec![people_rows(1..=2)]).await;
    g.set_rules(&[], &[]);
    let target = target();
    write_batch(&target, people_batch(&[(1, "old")]), "seed").await;
    g.ctx
        .server
        .set_column_auth("default", "merge_columns", vec!["id".into()]);
    // Key-only matching must not reduce a table source to a different select.
    assert_forbidden(merge(&target, &g.table, true).await.unwrap_err());
    assert_eq!(
        ids(&read_all(&target.new_read_builder()).await.unwrap()),
        [1]
    );
    g.ctx
        .server
        .set_column_auth("default", "merge_columns", vec!["id".into(), "name".into()]);
    merge(&target, &g.table, false).await.unwrap();
    g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
    let error = merge(&target, &g.table, true).await.unwrap_err();
    assert!(error.to_string().contains("unrestricted"), "{error}");
    assert_eq!(
        ids(&read_all(&target.new_read_builder()).await.unwrap()),
        [1, 2]
    );
}

#[tokio::test]
async fn unrestricted_pk_source_merges_versions_including_level_zero() {
    let g = written(
        "merge_pk_source",
        |options| {
            let mut builder = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
                .primary_key(["id"])
                .option("bucket", "1");
            for (key, value) in options {
                builder = builder.option(*key, *value);
            }
            builder.build().unwrap()
        },
        vec![people_batch(&[(1, "old"), (2, "bob")]), people_rows(1..=1)],
    )
    .await;
    g.set_rules(&[], &[]);
    let target = target();
    write_batch(&target, people_batch(&[(1, "target")]), "seed").await;
    merge(&target, &g.table, false).await.unwrap();
    assert_eq!(
        id_names(&read_all(&target.new_read_builder()).await.unwrap()),
        [(1, Some("alice".into())), (2, Some("bob".into()))]
    );
}

#[tokio::test]
async fn unrestricted_de_and_timestamp_window_sources_preserve_reader_semantics() {
    for data_evolution in [false, true] {
        let g = written(
            "merge_window_source",
            |options| {
                if data_evolution {
                    people(
                        &[
                            options,
                            &[
                                ("data-evolution.enabled", "true"),
                                ("row-tracking.enabled", "true"),
                                ("deletion-vectors.enabled", "true"),
                            ],
                        ]
                        .concat(),
                    )
                } else {
                    people(options)
                }
            },
            vec![people_rows(1..=1), people_rows(2..=2)],
        )
        .await;
        g.set_rules(&[], &[]);
        let source = if data_evolution {
            g.table.clone()
        } else {
            g.table.copy_with_options(HashMap::from([(
                "incremental-between-timestamp".into(),
                "0,9223372036854775807".into(),
            )]))
        };
        let target = target();
        write_batch(&target, people_batch(&[(1, "old")]), "seed").await;
        merge(&target, &source, false).await.unwrap();
        assert_eq!(
            id_names(&read_all(&target.new_read_builder()).await.unwrap()),
            [(1, Some("alice".into())), (2, Some("bob".into()))]
        );
        // Combined timestamp planning shares the same live grant.
        assert_eq!(
            g.ctx.server.auth_selects("default", "merge_window_source"),
            [None]
        );
    }
}
