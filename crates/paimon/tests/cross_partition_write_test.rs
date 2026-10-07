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

mod common;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::Table;
use std::sync::Arc;

async fn table(partition_in_key: bool, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("tenant", DataType::VarChar(VarCharType::string_type()))
        .column("p", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("v", DataType::Int(IntType::new()))
        .partition_keys(["tenant", "p"])
        .primary_key(if partition_in_key {
            vec!["tenant", "id"]
        } else {
            vec!["id"]
        })
        .option("bucket", "-1");
    if options.iter().any(|(key, _)| *key == "rowkind.field") {
        schema = schema.column("op", DataType::VarChar(VarCharType::string_type()));
    }
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let schema = TableSchema::new(0, &schema.build().unwrap());
    let (io, table) = memory_table("memory:/cross_partition_write", schema);
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn batch(rows: &[(&str, &str, i32, i32)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "tenant",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "p",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
        (
            "v",
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.3))) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn rows(table: &Table) -> Vec<(String, String, i32, i32)> {
    let read = table.new_read_builder();
    let plan = read.new_scan().with_scan_all_files().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let tenant = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let p = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let id = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let v = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| {
            (
                tenant.value(i).into(),
                p.value(i).into(),
                id.value(i),
                v.value(i),
            )
        }));
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn repeated_migrations_keep_the_last_input_even_when_returning_to_a_partition() {
    for chunk_size in [1, 2, 3, 4] {
        let table = table(false, &[]).await;
        let input = [
            ("t", "a", 1, 10),
            ("t", "b", 1, 20),
            ("t", "a", 1, 30),
            ("t", "a", 1, 40),
        ];
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        for chunk in input.chunks(chunk_size) {
            writer.write_arrow_batch(&batch(chunk)).await.unwrap();
        }
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            rows(&table).await,
            vec![("t".into(), "a".into(), 1, 40)],
            "chunk_size={chunk_size}"
        );
    }
}

#[tokio::test]
async fn global_index_keeps_partition_columns_of_composite_primary_keys() {
    let table = table(true, &[]).await;
    write_batch(&table, &batch(&[("x", "a", 1, 10), ("y", "a", 1, 20)])).await;
    assert_eq!(
        rows(&table).await,
        vec![
            ("x".into(), "a".into(), 1, 10),
            ("y".into(), "a".into(), 1, 20)
        ]
    );
    // Reconstructing the global index must retain both tenants too.
    write_batch(&table, &batch(&[("x", "b", 1, 30)])).await;
    assert_eq!(
        rows(&table).await,
        vec![
            ("x".into(), "b".into(), 1, 30),
            ("y".into(), "a".into(), 1, 20)
        ]
    );
}

#[tokio::test]
async fn dynamic_overwrite_keeps_global_index_for_migration_deletes() {
    let table = table(false, &[]).await;
    write_batch(&table, &batch(&[("t", "a", 1, 10), ("t", "c", 2, 20)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(&[("t", "b", 1, 30)]))
        .await
        .unwrap();
    builder
        .new_commit()
        .overwrite(writer.prepare_commit().await.unwrap(), None)
        .await
        .unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            ("t".into(), "b".into(), 1, 30),
            ("t".into(), "c".into(), 2, 20)
        ]
    );
    write_batch(&table, &batch(&[("t", "d", 1, 40)])).await;
    assert_eq!(
        rows(&table).await,
        vec![
            ("t".into(), "c".into(), 2, 20),
            ("t".into(), "d".into(), 1, 40)
        ]
    );
}

#[tokio::test]
async fn merge_engines_route_updates_like_java_after_restart() {
    for engine in ["deduplicate", "first-row", "partial-update", "aggregation"] {
        for restart in [false, true] {
            let mut options = vec![("merge-engine", engine)];
            if engine == "aggregation" {
                options.push(("fields.v.aggregate-function", "sum"));
            }
            let table = table(true, &options).await;
            let first = batch(&[("x", "a", 1, 10), ("y", "a", 1, 100)]);
            let second = batch(&[("x", "b", 1, 20), ("y", "b", 1, 200)]);
            if restart {
                write_batch(&table, &first).await;
                write_batch(&table, &second).await;
            } else {
                let builder = table.new_write_builder();
                let mut writer = builder.new_write().unwrap();
                writer.write_arrow_batch(&first).await.unwrap();
                writer.write_arrow_batch(&second).await.unwrap();
                builder
                    .new_commit()
                    .commit(writer.prepare_commit().await.unwrap())
                    .await
                    .unwrap();
            }
            let (partition, x, y) = match engine {
                "deduplicate" => ("b", 20, 200),
                "first-row" => ("a", 10, 100),
                "partial-update" => ("a", 20, 200),
                "aggregation" => ("a", 30, 300),
                _ => unreachable!(),
            };
            assert_eq!(
                rows(&table).await,
                vec![
                    ("x".into(), partition.into(), 1, x),
                    ("y".into(), partition.into(), 1, y)
                ],
                "engine={engine}, restart={restart}"
            );
        }
    }
}

fn with_kinds(rows: &[(&str, &str, i32, i32)], kinds: &[&str]) -> RecordBatch {
    let data = batch(rows);
    let mut fields = data.schema().fields().to_vec();
    fields.push(Arc::new(arrow_schema::Field::new(
        "op",
        arrow_schema::DataType::Utf8,
        true,
    )));
    let mut columns = data.columns().to_vec();
    columns.push(Arc::new(StringArray::from(kinds.to_vec())));
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn generated_row_kinds_keep_delete_and_update_order_during_migrations() {
    let table = table(false, &[("rowkind.field", "op")]).await;
    write_batch(&table, &with_kinds(&[("t", "a", 1, 10)], &["+I"])).await;
    write_batch(
        &table,
        &with_kinds(
            &[
                ("t", "b", 1, 20),
                ("t", "a", 1, 30),
                ("t", "b", 2, 40),
                ("t", "c", 2, 50),
            ],
            &["+U", "+U", "+I", "-D"],
        ),
    )
    .await;
    assert_eq!(rows(&table).await, vec![("t".into(), "a".into(), 1, 30)]);
    // The deleted key must not reappear when the global index is rebuilt.
    write_batch(&table, &with_kinds(&[("t", "c", 2, 60)], &["+I"])).await;
    assert_eq!(
        rows(&table).await,
        vec![
            ("t".into(), "a".into(), 1, 30),
            ("t".into(), "c".into(), 2, 60)
        ]
    );
}

#[tokio::test]
async fn bootstrap_rejects_duplicate_global_keys_before_writing() {
    let table = table(false, &[]).await;
    // Simulate existing files with the same full primary key in two partitions.
    // Such a table cannot supply an unambiguous global location on restart.
    let fixed = table.copy_with_options([("bucket".into(), "1".into())].into());
    write_batch(&fixed, &batch(&[("t", "a", 1, 10), ("t", "b", 1, 20)])).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    let error = writer
        .write_arrow_batch(&batch(&[("t", "c", 1, 30)]))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Duplicate primary key"),
        "{error}"
    );
    assert!(writer.prepare_commit().await.is_err());
    assert_eq!(rows(&table).await.len(), 2);
}

#[tokio::test]
async fn cross_partition_rejects_unsupported_index_configuration() {
    let table = table(false, &[]).await;
    for (key, value, message) in [
        ("bucket-key", "id", "Cannot define 'bucket-key'"),
        ("sequence.field", "v", "Cannot define 'sequence.field'"),
        ("cross-partition-upsert.index-ttl", "1 h", "index-ttl"),
    ] {
        let configured = table.copy_with_options([(key.into(), value.into())].into());
        match configured.new_write_builder().new_write() {
            Ok(_) => panic!("should reject {key}"),
            Err(error) => assert!(error.to_string().contains(message), "{error}"),
        }
    }
}
