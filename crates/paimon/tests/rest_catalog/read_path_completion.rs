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
use paimon::table::DataSplit;

#[tokio::test]
async fn historical_authorization_filters_and_masks_pinned_snapshots() {
    let g = written(
        "read_history",
        people,
        vec![people_rows(1..=2), people_rows(3..=4)],
    )
    .await;
    g.set_rules(
        &[int_leaf(0, "id", "GREATER_THAN", 1)],
        &[("name", upper(1, "name"))],
    );
    for snapshot_id in [None, Some(1), Some(2)] {
        let snapshot = match snapshot_id {
            Some(id) => Some(g.table.snapshot_manager().get_snapshot(id).await.unwrap()),
            None => None,
        };
        let view = g.table.copy_with_pinned_snapshot(snapshot.as_ref());
        let rows = read_all(&view.new_read_builder()).await.unwrap();
        let expected = match snapshot_id {
            None => vec![],
            Some(1) => vec![(2, Some("BOB".into()))],
            _ => vec![
                (2, Some("BOB".into())),
                (3, Some("CAROL".into())),
                (4, Some("DAVE".into())),
            ],
        };
        assert_eq!(id_names(&rows), expected);
    }
}

#[tokio::test]
async fn transported_splits_keep_captured_rules_and_require_live_authorization() {
    let g = written("transport", people, vec![people_rows(1..=3)]).await;
    g.set_rules(
        &[int_leaf(0, "id", "GREATER_THAN", 1)],
        &[("name", upper(1, "name"))],
    );
    let builder = g.table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let splits: Vec<_> = plan
        .splits()
        .iter()
        .map(|s| DataSplit::deserialize_split_v1(&s.serialize_split_v1().unwrap()).unwrap())
        .collect();
    let read = builder.new_read().unwrap();
    assert!(
        read.to_arrow(&splits).is_err(),
        "bytes alone do not grant access"
    );
    for relaxed in [false, true] {
        if relaxed {
            g.set_rules(&[], &[]);
        }
        let restored = read.authorize_splits(&splits).await.unwrap();
        let batches = read
            .to_arrow(&restored)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            id_names(&batches),
            [(2, Some("BOB".into())), (3, Some("CAROL".into()))]
        );
    }
    g.set_rules(&[int_leaf(0, "id", "EQUAL", 1)], &[]);
    assert!(
        read.authorize_splits(&splits).await.is_err(),
        "changed restrictions require a new plan"
    );
}

#[tokio::test]
async fn python_planned_data_splits_obtain_current_rules() {
    let g = written("unwrapped", people, vec![people_rows(1..=3)]).await;
    let plan = g.table.new_read_builder().new_scan().plan().await.unwrap();
    let mut splits = Vec::new();
    for split in plan.splits() {
        let mut json = serde_json::to_value(split).unwrap();
        json["query_auth_required"] = serde_json::json!(false);
        json["query_auth_response"] = serde_json::Value::Null;
        splits.push(serde_json::from_value(json).unwrap());
    }
    g.set_rules(&[int_leaf(0, "id", "EQUAL", 2)], &[]);
    let builder = g.table.new_read_builder();
    let read = builder.new_read().unwrap();
    let splits = read.authorize_splits(&splits).await.unwrap();
    let batches = read
        .to_arrow(&splits)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(ids(&batches), [2]);
}

#[tokio::test]
async fn old_files_with_dropped_stats_columns_remain_readable() {
    let g = written("dropped_stats", people, vec![people_rows(1..=3)]).await;
    let current = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("query-auth.enabled", "true")
        .build()
        .unwrap();
    g.ctx
        .server
        .set_table_schema_id("default", "dropped_stats", current, 1);
    let table = g.ctx.catalog.get_table(&g.identifier).await.unwrap();
    g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    assert!(plan
        .splits()
        .iter()
        .all(|s| s.data_files().iter().all(|f| f.schema_id == 0)));
    let rows = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(ids(&rows), [2, 3]);
}

#[tokio::test]
async fn branch_authorization_uses_the_decorated_identifier() {
    let g = guarded("main_auth", &["id"]).await;
    g.ctx.server.add_table_with_schema(
        "default",
        "main_auth$branch_dev",
        schema_of(&["id"], GUARDED),
        g.table.location(),
    );
    let branch = g
        .ctx
        .catalog
        .get_table(&Identifier::new("default", "main_auth$branch_dev"))
        .await
        .unwrap();
    branch.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(
        g.ctx.server.auth_selects("default", "main_auth$branch_dev"),
        [None]
    );
    assert!(g.ctx.server.auth_selects("default", "main_auth").is_empty());
}

#[tokio::test]
async fn write_planning_rejects_rules_and_keeps_all_file_scan_guard() {
    let g = written("write_plan", people, vec![people_rows(1..=2)]).await;
    assert!(!g
        .table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .unwrap()
        .splits()
        .is_empty());
    g.ctx
        .server
        .set_column_auth("default", "write_plan", vec!["id".into()]);
    let mut projected = g.table.new_read_builder();
    projected.with_projection(&["id"]).unwrap();
    assert_forbidden(projected.new_scan().plan_all().await.unwrap_err());
    g.ctx
        .server
        .set_column_auth("default", "write_plan", vec!["id".into(), "name".into()]);
    g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
    assert!(g
        .table
        .new_read_builder()
        .new_scan()
        .plan_all()
        .await
        .is_err());
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
async fn historical_schema_rules_use_null_for_new_columns_and_reject_renames() {
    let g = written("schema_history", people, vec![people_rows(1..=3)]).await;
    let mut value = serde_json::to_value(people(GUARDED)).unwrap();
    value["fields"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"id":2,"name":"n","type":"INT"}));
    let current: Schema = serde_json::from_value(value.clone()).unwrap();
    g.ctx
        .server
        .set_table_schema_id("default", "schema_history", current, 1);
    let latest = g.ctx.catalog.get_table(&g.identifier).await.unwrap();
    let historical = latest
        .copy_with_time_travel_strict(HashMap::from([("scan.snapshot-id".into(), "1".into())]))
        .await
        .unwrap();
    assert_eq!(historical.schema().id(), 0);
    assert_eq!(historical.schema().fields().len(), 2);
    g.set_rules(&[serde_json::json!({"kind":"LEAF","function":"IS_NULL","literals":[],"transform":{"name":"FIELD_REF","fieldRef":{"name":"n","type":"INT","index":2}}})], &[]);
    assert_eq!(
        ids(&read_all(&historical.new_read_builder()).await.unwrap()),
        [1, 2, 3]
    );
    g.set_rules(&[int_leaf(2, "n", "GREATER_THAN", 1)], &[]);
    assert!(ids(&read_all(&historical.new_read_builder()).await.unwrap()).is_empty());
    value["fields"][0]["name"] = serde_json::json!("renamed");
    g.ctx.server.set_table_schema_id(
        "default",
        "schema_history",
        serde_json::from_value(value).unwrap(),
        2,
    );
    g.set_rules(&[int_leaf(0, "renamed", "GREATER_THAN", 1)], &[]);
    assert!(historical
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .is_err());
}

#[tokio::test]
async fn fuse_file_io_keeps_rest_identity_and_original_metadata_paths() {
    for mode in ["pvfs", "raw"] {
        let g = written("mount_read", people, vec![people_rows(1..=3)]).await;
        let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
        let local = std::path::PathBuf::from(g.table.location());
        let root = g._tmp.path().join("fuse mount%");
        let mounted = match mode {
            "pvfs" => root.join("default/mount_read"),
            _ => root.join("warehouse/default/mount_read"),
        };
        std::fs::create_dir_all(mounted.parent().unwrap()).unwrap();
        std::fs::rename(local, &mounted).unwrap();
        let remote = "memory:/warehouse/default/mount_read";
        g.ctx
            .server
            .add_table_with_schema("default", "mount_read", people(GUARDED), remote);
        g.ctx
            .server
            .set_table_external("default", "mount_read", false);
        let mut options = g.ctx.catalog.options().clone();
        options.set("fuse.enabled", "true");
        let root_option = if mode == "raw" {
            url::Url::from_file_path(&root).unwrap().to_string()
        } else {
            root.to_str().unwrap().to_string()
        };
        options.set("fuse.root", root_option);
        options.set("fuse.mode", mode);
        options.set("fuse.validation-mode", "none");
        // There is no token endpoint: any accidental token request fails.
        options.set("data-token.enabled", "true");
        let catalog = RESTCatalog::new(options.clone(), false).await.unwrap();
        let response = catalog
            .get_table(&g.identifier)
            .await
            .unwrap()
            .rest_env()
            .unwrap()
            .api()
            .get_table(&g.identifier)
            .await
            .unwrap();
        let tables = [
            catalog.get_table(&g.identifier).await.unwrap(),
            Table::from_rest_response(g.identifier.clone(), response, options)
                .await
                .unwrap(),
        ];
        g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
        for table in tables {
            // A Python-resolved snapshot must still read all files through FUSE.
            let table = table.copy_with_pinned_snapshot(Some(&snapshot));
            assert_eq!(table.location(), remote);
            assert_eq!(table.rest_env().unwrap().identifier(), &g.identifier);
            assert_eq!(
                ids(&read_all(&table.new_read_builder()).await.unwrap()),
                [2, 3]
            );
            table
                .file_io()
                .new_output(&format!("{remote}/sidecar"))
                .unwrap()
                .write(bytes::Bytes::from_static(b"mounted write"))
                .await
                .unwrap();
            assert_eq!(
                std::fs::read(mounted.join("sidecar")).unwrap(),
                b"mounted write"
            );
        }
    }
}

#[tokio::test]
async fn fuse_validation_modes_control_mount_fallback() {
    for mode in ["strict", "warn", "none"] {
        let g = written("fuse_validation", people, vec![people_rows(1..=2)]).await;
        g.ctx
            .server
            .set_table_external("default", "fuse_validation", false);
        g.ctx
            .server
            .set_database_location("default", Some("memory:/warehouse/default"));
        let mut options = g.ctx.catalog.options().clone();
        options.set("fuse.enabled", "true");
        options.set("fuse.root", g._tmp.path().join("missing").to_str().unwrap());
        options.set("fuse.validation-mode", mode);
        let catalog = RESTCatalog::new(options, false).await.unwrap();
        match mode {
            "strict" => assert!(catalog
                .get_table(&g.identifier)
                .await
                .unwrap_err()
                .to_string()
                .contains("FUSE local path validation failed")),
            "warn" => {
                let table = catalog.get_table(&g.identifier).await.unwrap();
                assert_eq!(
                    ids(&read_all(&table.new_read_builder()).await.unwrap()),
                    [1, 2]
                );
                // A failed validation is cached; do not retry it on every table.
                g.ctx.server.add_no_permission_database("default");
                catalog.get_table(&g.identifier).await.unwrap();
                table
                    .rest_env()
                    .unwrap()
                    .get_table(&g.identifier)
                    .await
                    .unwrap();
            }
            _ => {
                g.ctx.server.add_no_permission_database("default");
                catalog.get_table(&g.identifier).await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn fuse_validation_caches_success_but_never_swallows_rest_errors() {
    let g = written("fuse_validation", people, vec![]).await;
    g.ctx
        .server
        .set_table_external("default", "fuse_validation", false);
    g.ctx
        .server
        .set_database_location("default", Some("memory:/warehouse/default"));
    let root = g._tmp.path().join("fuse mount%");
    std::fs::create_dir_all(root.join("default")).unwrap();
    let mut options = g.ctx.catalog.options().clone();
    options.set("fuse.enabled", "true");
    options.set(
        "fuse.root",
        url::Url::from_file_path(&root).unwrap().to_string(),
    );
    let catalog = RESTCatalog::new(options.clone(), false).await.unwrap();
    let table = catalog.get_table(&g.identifier).await.unwrap();
    g.ctx.server.add_no_permission_database("default");
    catalog.get_table(&g.identifier).await.unwrap();
    table
        .rest_env()
        .unwrap()
        .get_table(&g.identifier)
        .await
        .unwrap();
    for mode in ["strict", "warn"] {
        options.set("fuse.validation-mode", mode);
        let fresh = RESTCatalog::new(options.clone(), false).await.unwrap();
        let error = fresh.get_table(&g.identifier).await.unwrap_err();
        assert!(matches!(error, paimon::Error::RestApi { .. }));
    }
}

#[tokio::test]
async fn external_tables_ignore_fuse_and_invalid_fuse_options_fail_for_internal_tables() {
    let g = written("external_fuse", people, vec![]).await;
    let mut options = g.ctx.catalog.options().clone();
    options.set("fuse.enabled", "true");
    let catalog = RESTCatalog::new(options.clone(), false).await.unwrap();
    // The fixture is external. Its normal FileIO never resolves a FUSE mount.
    catalog.get_table(&g.identifier).await.unwrap();
    g.ctx
        .server
        .set_table_external("default", "external_fuse", false);
    assert!(catalog
        .get_table(&g.identifier)
        .await
        .unwrap_err()
        .to_string()
        .contains("fuse.root"));
    options.set("fuse.root", g._tmp.path().to_str().unwrap());
    options.set("fuse.validation-mode", "invalid");
    let catalog = RESTCatalog::new(options, false).await.unwrap();
    assert!(catalog
        .get_table(&g.identifier)
        .await
        .unwrap_err()
        .to_string()
        .contains("fuse.validation-mode"));
}

pub(super) fn transport(plan: &paimon::table::Plan) -> Vec<DataSplit> {
    plan.splits()
        .iter()
        .map(|split| DataSplit::deserialize_split_v1(&split.serialize_split_v1().unwrap()).unwrap())
        .collect()
}

#[tokio::test]
async fn restored_splits_cannot_substitute_paths_metadata_vectors_or_raw_mode() {
    let g = written("restore_provenance", people, vec![people_rows(1..=2)]).await;
    let builder = g.table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let read = builder.new_read().unwrap();
    let splits = transport(&plan);
    for (field, value) in [
        (
            "bucket_path",
            serde_json::json!("file:///other/table/bucket-0"),
        ),
        ("snapshot_id", serde_json::json!(5678)),
        (
            "data_deletion_files",
            serde_json::json!([{"path":"file:///other/dv","offset":0,"length":1}]),
        ),
    ] {
        let mut json = serde_json::to_value(&splits[0]).unwrap();
        json[field] = value;
        let forged: DataSplit = serde_json::from_value(json).unwrap();
        assert!(read.authorize_splits(&[forged]).await.is_err(), "{field}");
    }
    for field in ["_FILE_NAME", "_EXTERNAL_PATH"] {
        let mut json = serde_json::to_value(&splits[0]).unwrap();
        json["data_files"][0][field] = serde_json::json!("file:///other/private.parquet");
        let forged: DataSplit = serde_json::from_value(json).unwrap();
        assert!(read.authorize_splits(&[forged]).await.is_err(), "{field}");
    }
}

#[tokio::test]
async fn authorized_pinned_snapshots_must_match_the_tables_snapshot() {
    let g = written(
        "pinned_metadata",
        people,
        vec![people_rows(1..=1), people_rows(2..=2)],
    )
    .await;
    let first = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
    let second = g.table.snapshot_manager().get_snapshot(2).await.unwrap();
    let mut metadata = serde_json::to_value(&first).unwrap();
    // A captured snapshot id must not accidentally be paired with another
    // snapshot's manifest list when crossing the JSON boundary.
    metadata["deltaManifestList"] = serde_json::json!(second.delta_manifest_list());
    metadata["deltaManifestListSize"] = serde_json::json!(second.delta_manifest_list_size());
    let mixed: paimon::spec::Snapshot = serde_json::from_value(metadata).unwrap();
    let view = g.table.copy_with_pinned_snapshot(Some(&mixed));
    let error = view.new_read_builder().new_scan().plan().await.unwrap_err();
    assert!(error.to_string().contains("pinned snapshot"), "{error}");
}

#[tokio::test]
async fn authorized_pinned_snapshot_can_verify_a_retained_tag_after_expiry_or_id_reuse() {
    for replaced in [false, true] {
        let g = written(
            "pinned_tag",
            people,
            vec![people_rows(1..=1), people_rows(2..=2)],
        )
        .await;
        let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
        g.table
            .tag_manager()
            .create("before", &snapshot)
            .await
            .unwrap();
        let path = g.table.snapshot_manager().snapshot_path(1);
        if replaced {
            let later = g.table.snapshot_manager().get_snapshot(2).await.unwrap();
            let mut json = serde_json::to_value(later).unwrap();
            json["id"] = serde_json::json!(1);
            g.table
                .file_io()
                .new_output(&path)
                .unwrap()
                .write(bytes::Bytes::from(serde_json::to_vec(&json).unwrap()))
                .await
                .unwrap();
        } else {
            g.table.file_io().delete_file(&path).await.unwrap();
        }
        let view = g.table.copy_with_pinned_snapshot(Some(&snapshot));
        let rows = read_all(&view.new_read_builder()).await.unwrap();
        assert_eq!(ids(&rows), [1]);
    }
}

#[tokio::test]
async fn authorized_pinned_latest_can_use_rest_when_tag_metadata_is_unavailable() {
    let g = written("pinned_rest", people, vec![people_rows(1..=1)]).await;
    g.ctx
        .server
        .set_table_external("default", "pinned_rest", false);
    let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
    let source = std::path::PathBuf::from(g.table.location());
    let root = tempfile::tempdir().unwrap();
    let mounted = root.path().join("default/pinned_rest");
    for directory in ["manifest", "bucket-0"] {
        let destination = mounted.join(directory);
        std::fs::create_dir_all(&destination).unwrap();
        for file in std::fs::read_dir(source.join(directory)).unwrap() {
            let file = file.unwrap();
            std::fs::copy(file.path(), destination.join(file.file_name())).unwrap();
        }
    }
    // The mount exposes data/manifests but no snapshot directory. An unrelated
    // malformed tag must not block validation through the REST snapshot source.
    std::fs::create_dir_all(mounted.join("tag")).unwrap();
    std::fs::write(mounted.join("tag/tag-broken"), "not snapshot JSON").unwrap();
    let mut options = g.ctx.catalog.options().clone();
    options.set("fuse.enabled", "true");
    options.set("fuse.root", root.path().to_str().unwrap());
    options.set("fuse.validation-mode", "none");
    let table = RESTCatalog::new(options, false)
        .await
        .unwrap()
        .get_table(&g.identifier)
        .await
        .unwrap();
    assert!(table.tag_manager().list_all().await.is_err());
    let view = table.copy_with_pinned_snapshot(Some(&snapshot));
    let rows = read_all(&view.new_read_builder()).await.unwrap();
    assert_eq!(ids(&rows), [1]);
}

#[tokio::test]
async fn authorized_repin_ignores_log_offsets_absent_from_the_python_transport() {
    let g = written(
        "pinned_offsets",
        people,
        vec![people_rows(1..=1), people_rows(2..=2)],
    )
    .await;
    let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
    let mut json = serde_json::to_value(&snapshot).unwrap();
    json["logOffsets"] = serde_json::json!({"0": 42});
    g.table
        .file_io()
        .new_output(&g.table.snapshot_manager().snapshot_path(1))
        .unwrap()
        .write(bytes::Bytes::from(serde_json::to_vec(&json).unwrap()))
        .await
        .unwrap();
    let view = g
        .table
        .copy_with_time_travel_strict(HashMap::from([("scan.snapshot-id".into(), "1".into())]))
        .await
        .unwrap();
    let rows = read_all(&view.new_read_builder()).await.unwrap();
    assert_eq!(ids(&rows), [1]);
    g.table
        .file_io()
        .delete_file(&g.table.snapshot_manager().snapshot_path(1))
        .await
        .unwrap();
    // Snapshot is the equivalent Python read view, without logOffsets.
    let view = view.copy_with_pinned_snapshot(Some(&snapshot));
    let rows = read_all(&view.new_read_builder()).await.unwrap();
    assert_eq!(ids(&rows), [1]);
}

#[tokio::test]
async fn retained_snapshot_authorization_and_selector_replacement() {
    let g = written(
        "retained",
        people,
        vec![people_rows(1..=1), people_rows(2..=2)],
    )
    .await;
    let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
    let view = g.table.copy_with_pinned_snapshot(Some(&snapshot));
    let builder = view.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let read = builder.new_read().unwrap();
    let splits = transport(&plan);
    let schema = view
        .schema()
        .copy_with_options(HashMap::from([("scan.snapshot-id".into(), "2".into())]));
    let changed = view.copy_with_resolved_schema(schema, "main").unwrap();
    // Changing a pinned selector invalidates its old cache; this copy must
    // either re-resolve or fail, never silently return snapshot 1's rows.
    assert!(changed.new_read_builder().new_scan().plan().await.is_err());
    let other = g.table.copy_with_pinned_snapshot(Some(
        &g.table.snapshot_manager().get_snapshot(2).await.unwrap(),
    ));
    assert!(other
        .new_read_builder()
        .new_read()
        .unwrap()
        .authorize_splits(&splits)
        .await
        .is_err());
    g.table
        .file_io()
        .delete_file(&g.table.snapshot_manager().snapshot_path(1))
        .await
        .unwrap();
    let recopied = view.copy_with_pinned_snapshot(Some(&snapshot));
    let rows = read_all(&recopied.new_read_builder()).await.unwrap();
    assert_eq!(ids(&rows), [1]);
    let restored = read.authorize_splits(&splits).await.unwrap();
    let rows = read
        .to_arrow(&restored)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(ids(&rows), [1]);
}

#[tokio::test]
async fn restored_partition_predicates_require_permission_and_unmasked_values() {
    let schema = |options: &[(&str, &str)]| {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("region", DataType::VarChar(VarCharType::string_type()))
            .partition_keys(["region"])
            .options(
                options
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string())),
            )
            .build()
            .unwrap()
    };
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2])) as arrow_array::ArrayRef,
        ),
        (
            "region",
            Arc::new(StringArray::from(vec!["secret", "public"])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    let g = written("restore_partition", schema, vec![batch]).await;
    let plan = g.table.new_read_builder().new_scan().plan().await.unwrap();
    let splits = transport(&plan);
    // A transported split still requires rules when this freshly loaded
    // reader predates/does not enable query-auth locally. Its partition
    // predicate follows the ordinary split-metadata path.
    g.ctx
        .server
        .set_table_schema_id("default", "restore_partition", schema(&[]), 0);
    let unmarked = g.ctx.catalog.get_table(&g.identifier).await.unwrap();
    let pb = PredicateBuilder::new(unmarked.schema().fields());
    let mut builder = unmarked.new_read_builder();
    builder
        .with_projection(&["id"])
        .unwrap()
        .with_filter(pb.equal("region", Datum::String("secret".into())).unwrap());
    let read = builder.new_read().unwrap();
    g.ctx
        .server
        .set_column_auth("default", "restore_partition", vec!["id".into()]);
    assert!(read.authorize_splits(&splits).await.is_err());
    g.ctx.server.set_column_auth(
        "default",
        "restore_partition",
        vec!["id".into(), "region".into()],
    );
    g.set_rules(&[], &[("region", upper(1, "region"))]);
    let restored = read.authorize_splits(&splits).await.unwrap_err();
    assert!(restored.to_string().contains("transported rules differ"));
    let current = transport(&g.table.new_read_builder().new_scan().plan().await.unwrap());
    assert!(
        read.authorize_splits(&current).await.is_err(),
        "raw partition selection must not bypass masking"
    );
}

#[tokio::test]
async fn transported_stream_delta_is_verified_separately_from_snapshot_mode() {
    let g = written(
        "stream_restore",
        people,
        vec![people_rows(1..=2), people_rows(3..=4)],
    )
    .await;
    g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
    let mut scan = g.table.new_stream_scan().unwrap();
    scan.restore(Some(2)).unwrap();
    let plan = scan.plan().await.unwrap().unwrap();
    assert!(plan.splits().iter().all(DataSplit::is_streaming));
    let splits = transport(&plan);
    let builder = g.table.new_read_builder();
    let read = builder.new_read().unwrap();
    let restored = read.authorize_splits(&splits).await.unwrap();
    let rows = read
        .to_arrow(&restored)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(ids(&rows), [3, 4]);
    // An ordinary state file not emitted by snapshot 2's delta cannot be
    // relabelled as a streaming event, even after a valid snapshot split.
    let full = transport(&builder.new_scan().plan().await.unwrap());
    let old = full
        .iter()
        .find(|s| s.data_files().iter().any(|f| f.min_sequence_number == 0))
        .unwrap();
    let mut json = serde_json::to_value(old).unwrap();
    json["is_streaming"] = serde_json::json!(true);
    let fake: DataSplit = serde_json::from_value(json).unwrap();
    assert!(read.authorize_splits(&[old.clone(), fake]).await.is_err());
}

#[tokio::test]
async fn transported_primary_key_splits_cannot_drop_merge_inputs_or_read_them_raw() {
    let schema = |options: &[(&str, &str)]| {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .options(
                options
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string())),
            )
            .build()
            .unwrap()
    };
    let g = written(
        "pk_restore",
        schema,
        vec![people_rows(1..=2), people_rows(1..=2)],
    )
    .await;
    let builder = g.table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let splits = transport(&plan);
    assert_eq!(splits.len(), 1);
    assert!(!splits[0].raw_convertible());
    let read = builder.new_read().unwrap();
    for mode in ["raw", "omit", "duplicate"] {
        let mut json = serde_json::to_value(&splits[0]).unwrap();
        match mode {
            "raw" => json["raw_convertible"] = serde_json::json!(true),
            "omit" => {
                json["data_files"].as_array_mut().unwrap().pop();
            }
            _ => {
                let file = json["data_files"][0].clone();
                json["data_files"].as_array_mut().unwrap().push(file);
            }
        }
        let forged: DataSplit = serde_json::from_value(json).unwrap();
        assert!(read.authorize_splits(&[forged]).await.is_err());
    }
    let restored = read.authorize_splits(&splits).await.unwrap();
    assert_eq!(
        ids(&read
            .to_arrow(&restored)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()),
        [1, 2]
    );
}

#[tokio::test]
async fn transported_rules_allow_reordered_equivalent_conjunctions() {
    let g = written("reordered_rules", people, vec![people_rows(1..=4)]).await;
    let a = int_leaf(0, "id", "GREATER_THAN", 1);
    let b = int_leaf(0, "id", "LESS_THAN", 4);
    g.set_rules(&[a.clone(), b.clone()], &[]);
    let builder = g.table.new_read_builder();
    let splits = transport(&builder.new_scan().plan().await.unwrap());
    let reader = builder.new_read().unwrap();
    for filters in [
        vec![b.clone(), a.clone()],
        vec![
            serde_json::json!({"kind":"COMPOUND","function":"AND","children":[b.clone(),a.clone()]}),
        ],
    ] {
        g.set_rules(&filters, &[]);
        let restored = reader.authorize_splits(&splits).await.unwrap();
        assert_eq!(
            ids(&reader
                .to_arrow(&restored)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()),
            [2, 3]
        );
    }
}

#[tokio::test]
async fn transported_optional_bucket_layout_uses_canonical_snapshot_paths() {
    let g = written("optional_layout", people, vec![people_rows(1..=2)]).await;
    let builder = g.table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let mut splits = transport(&plan);
    for split in &mut splits {
        let mut json = serde_json::to_value(&split).unwrap();
        json["total_buckets"] = serde_json::Value::Null;
        if split.bucket_path().starts_with('/') {
            json["bucket_path"] = serde_json::json!(format!("file://{}", split.bucket_path()));
        }
        *split = serde_json::from_value(json).unwrap();
    }
    let reader = builder.new_read().unwrap();
    let restored = reader.authorize_splits(&splits).await.unwrap();
    assert!(restored
        .iter()
        .zip(plan.splits())
        .all(|(restored, original)| { restored.bucket_path() == original.bucket_path() }));
    assert_eq!(
        ids(&reader
            .to_arrow(&restored)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()),
        [1, 2]
    );
}

#[tokio::test]
async fn file_scan_requires_unrestricted_whole_table_authorization() {
    let g = written("raw_files", people, vec![people_rows(1..=2)]).await;
    let mut projected = g.table.new_read_builder();
    projected.with_projection(&["id"]).unwrap().with_limit(0);
    let files = projected.new_scan().plan_files().await.unwrap();
    assert_eq!(files.snapshot_id(), Some(1));
    assert!(
        !files.files().is_empty(),
        "row LIMIT must not hide metadata"
    );
    assert_eq!(
        g.ctx.server.auth_selects("default", "raw_files").last(),
        Some(&None)
    );

    g.ctx
        .server
        .set_column_auth("default", "raw_files", vec!["id".into()]);
    assert_forbidden(projected.new_scan().plan_files().await.unwrap_err());
    g.ctx
        .server
        .set_column_auth("default", "raw_files", vec!["id".into(), "name".into()]);
    for (filters, masks) in [
        (vec![int_leaf(0, "id", "GREATER_THAN", 1)], vec![]),
        (vec![], vec![("name", upper(1, "name"))]),
    ] {
        g.set_rules(&filters, &masks);
        assert!(projected.new_scan().plan_files().await.is_err());
        let empty = g.table.copy_with_options(HashMap::from([
            ("incremental-between-timestamp".into(), "100,100".into()),
            ("scan.mode".into(), "incremental".into()),
        ]));
        assert!(
            empty
                .new_read_builder()
                .new_scan()
                .plan_files()
                .await
                .is_err(),
            "empty timestamp windows must authorize before returning metadata"
        );
    }
}

#[tokio::test]
async fn file_scan_refuses_a_replaced_schema() {
    let g = guarded("file_schema", &["id"]).await;
    let replaced = g
        .table
        .copy_with_resolved_schema(
            paimon::spec::TableSchema::new(0, &schema_of(&["id", "secret"], GUARDED)),
            "main",
        )
        .unwrap();
    assert_refused(
        replaced
            .new_read_builder()
            .new_scan()
            .plan_files()
            .await
            .unwrap_err(),
    );
    g.ctx.server.set_table_schema_id(
        "default",
        "file_schema",
        schema_of(&["id", "extra"], GUARDED),
        0,
    );
    assert_drifted(
        g.table
            .new_read_builder()
            .new_scan()
            .plan_files()
            .await
            .unwrap_err(),
        "serves other columns",
    );
}

#[tokio::test]
async fn file_scan_checks_identity_after_planning_empty_file_sets() {
    for timestamp_window in [false, true] {
        let name = if timestamp_window {
            "empty_file_window"
        } else {
            "empty_file_table"
        };
        let g = guarded(name, &["id"]).await;
        let selected = if timestamp_window {
            g.table.copy_with_options(HashMap::from([
                ("incremental-between-timestamp".into(), "100,100".into()),
                ("scan.mode".into(), "incremental".into()),
            ]))
        } else {
            g.table.clone()
        };
        g.ctx
            .server
            .set_table_uuid_after_calls("default", name, "replacement-after-grant", 2);
        assert_drifted(
            selected
                .new_read_builder()
                .new_scan()
                .plan_files()
                .await
                .unwrap_err(),
            "now resolves to uuid",
        );
    }
}
