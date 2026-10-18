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
        .plan_for_write()
        .await
        .unwrap()
        .splits()
        .is_empty());
    g.ctx
        .server
        .set_column_auth("default", "write_plan", vec!["id".into()]);
    let mut projected = g.table.new_read_builder();
    projected.with_projection(&["id"]).unwrap();
    assert_forbidden(projected.new_scan().plan_for_write().await.unwrap_err());
    g.ctx
        .server
        .set_column_auth("default", "write_plan", vec!["id".into(), "name".into()]);
    g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
    assert!(g
        .table
        .new_read_builder()
        .new_scan()
        .plan_for_write()
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
async fn mounted_file_io_keeps_rest_identity_and_original_metadata_paths() {
    let g = written("mount_read", people, vec![people_rows(1..=3)]).await;
    let local = url::Url::parse(g.table.location())
        .unwrap()
        .to_file_path()
        .unwrap();
    let remote = "memory:/mounted/table";
    g.ctx
        .server
        .add_table_with_schema("default", "mount_read", people(GUARDED), remote);
    let table = g.ctx.catalog.get_table(&g.identifier).await.unwrap();
    let table = table.copy_with_file_io(
        table
            .file_io()
            .clone()
            .with_path_mapping(remote, local.to_str().unwrap())
            .unwrap(),
    );
    g.set_rules(&[int_leaf(0, "id", "GREATER_THAN", 1)], &[]);
    // REST snapshot lookup has no mounted filesystem; use the captured snapshot
    // from the same table's mount, just as a Python-resolved read view does.
    let snapshot = g.table.snapshot_manager().get_snapshot(1).await.unwrap();
    let table = table.copy_with_pinned_snapshot(Some(&snapshot));
    assert_eq!(table.location(), remote);
    assert_eq!(table.rest_env().unwrap().identifier(), &g.identifier);
    assert_eq!(
        ids(&read_all(&table.new_read_builder()).await.unwrap()),
        [2, 3]
    );
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
        json["bucket_path"] =
            serde_json::json!(split.bucket_path().strip_prefix("file://").unwrap());
        *split = serde_json::from_value(json).unwrap();
    }
    let reader = builder.new_read().unwrap();
    let restored = reader.authorize_splits(&splits).await.unwrap();
    assert!(restored
        .iter()
        .all(|split| split.bucket_path().starts_with("file://")));
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
