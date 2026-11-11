// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;

#[tokio::test]
async fn row_id_updates_validate_catalog_fields_without_current_schema_files() {
    for change in ["rename", "add", "recreate"] {
        for advance in ["none", "append", "update"] {
            let ctx = setup_catalog(vec!["default"]).await;
            let directory = tempfile::tempdir().unwrap();
            let name = format!("row_id_schema_{change}_{advance}");
            let schema = schema_of(
                &["id", "value"],
                &[
                    ("data-evolution.enabled", "true"),
                    ("row-tracking.enabled", "true"),
                    ("write-only", "true"),
                ],
            );
            ctx.server.add_table_with_schema(
                "default",
                &name,
                schema.clone(),
                directory.path().to_str().unwrap(),
            );
            let identifier = Identifier::new("default", &name);
            let table = ctx.catalog.get_table(&identifier).await.unwrap();
            let check_table = ctx.catalog.get_table(&identifier).await.unwrap();
            let batch = RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![10])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap();
            let mut writer = table.new_write_builder().new_write().unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
            table
                .new_write_builder()
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            let input = RecordBatch::try_from_iter([
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![0])) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![99])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap();
            let messages = table
                .new_write_builder()
                .new_update()
                .unwrap()
                .update_by_arrow_with_row_id(vec![input])
                .await
                .unwrap();
            let mut evolved = serde_json::to_value(schema).unwrap();
            match change {
                "rename" => evolved["fields"][1]["name"] = "renamed".into(),
                "recreate" => evolved["fields"][1]["id"] = 2.into(),
                _ => evolved["fields"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({"id": 2, "name": "extra", "type": "INT"})),
            }
            ctx.server.set_table_schema_id(
                "default",
                &name,
                serde_json::from_value(evolved).unwrap(),
                1,
            );
            let current = ctx.catalog.get_table(&identifier).await.unwrap();
            assert!(current.schema_manager().latest().await.unwrap().is_none());
            if advance == "append" {
                let input = RecordBatch::try_new(
                    paimon::arrow::build_target_arrow_schema(current.schema().fields()).unwrap(),
                    current
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| {
                            Arc::new(Int32Array::from(vec![if field.name() == "id" {
                                2
                            } else {
                                20
                            }])) as arrow_array::ArrayRef
                        })
                        .collect(),
                )
                .unwrap();
                let mut writer = current.new_write_builder().new_write().unwrap();
                writer.write_arrow_batch(&input).await.unwrap();
                current
                    .new_write_builder()
                    .new_commit()
                    .commit(writer.prepare_commit().await.unwrap())
                    .await
                    .unwrap();
            }
            if advance == "update" {
                // The independent schema-1 writer needs the historical source
                // definition to read schema-0 rows. Keep the current schema
                // catalog-only, so old committers must resolve it through REST.
                table
                    .file_io()
                    .mkdirs(&format!("{}/schema", table.location()))
                    .await
                    .unwrap();
                table
                    .file_io()
                    .new_output(&format!("{}/schema/schema-0", table.location()))
                    .unwrap()
                    .write(serde_json::to_vec(table.schema()).unwrap().into())
                    .await
                    .unwrap();
                let input = RecordBatch::try_from_iter([
                    (
                        "_ROW_ID",
                        Arc::new(Int64Array::from(vec![0])) as arrow_array::ArrayRef,
                    ),
                    (
                        "id",
                        Arc::new(Int32Array::from(vec![123])) as arrow_array::ArrayRef,
                    ),
                ])
                .unwrap();
                let concurrent = current
                    .new_write_builder()
                    .new_update()
                    .unwrap()
                    .update_by_arrow_with_row_id(vec![input])
                    .await
                    .unwrap();
                current
                    .new_write_builder()
                    .new_commit()
                    .commit(concurrent)
                    .await
                    .unwrap();
            }
            assert!(!table
                .file_io()
                .exists(&format!("{}/schema/schema-1", table.location()))
                .await
                .unwrap());
            let before = table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap()
                .unwrap();
            let check = check_table
                .new_write_builder()
                .new_commit()
                .check_row_id_conflicts(&messages, before, 1)
                .await;
            // Use an independent old handle so the check cannot populate the
            // committer's schema cache before its own conflict detection.
            let result = table
                .new_write_builder()
                .new_commit()
                .commit(messages)
                .await;
            if change == "recreate" {
                assert!(check.unwrap_err().to_string().contains("dropped field"));
                assert!(result.unwrap_err().to_string().contains("dropped field"));
                assert_eq!(
                    current
                        .snapshot_manager()
                        .get_latest_snapshot_id()
                        .await
                        .unwrap(),
                    Some(before)
                );
            } else {
                check.unwrap();
                result.unwrap();
                let snapshot = current
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(snapshot.id(), before + 1);
                assert_eq!(snapshot.schema_id(), 1);
                let builder = table.new_read_builder();
                let plan = builder.new_scan().plan().await.unwrap();
                let batches: Vec<RecordBatch> = builder
                    .new_read()
                    .unwrap()
                    .to_arrow(plan.splits())
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let mut rows: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        column::<Int32Array>(batch, "id")
                            .values()
                            .iter()
                            .copied()
                            .zip(
                                column::<Int32Array>(batch, "value")
                                    .values()
                                    .iter()
                                    .copied(),
                            )
                    })
                    .collect();
                rows.sort_unstable();
                let expected = match advance {
                    "append" => vec![(1, 99), (2, 20)],
                    "update" => vec![(123, 99)],
                    _ => vec![(1, 99)],
                };
                assert_eq!(rows, expected, "{change}: {advance}");
            }
        }
    }
}
