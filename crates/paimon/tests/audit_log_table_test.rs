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

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use paimon::spec::{
    DataType, IntType, Schema, TableSchema, VarCharType, ROW_KIND_FIELD_ID, ROW_KIND_FIELD_NAME,
    SEQUENCE_NUMBER_FIELD_NAME,
};
use paimon::table::{AuditLogTable, IncrementalScanMode};

use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, memory_table, persist_table_schema, pk_schema, setup_dirs,
    write_batch,
};

fn collect_audit_rows(batches: &[RecordBatch]) -> Vec<(String, i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let kind_idx = schema.index_of("rowkind").unwrap();
        let id_idx = schema.index_of("id").unwrap();
        let value_idx = schema.index_of("value").unwrap();
        let kinds = batch
            .column(kind_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                kinds.value(row).to_string(),
                ids.value(row),
                values.value(row),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

fn collect_audit_rows_with_sequence(batches: &[RecordBatch]) -> Vec<(String, i64, i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let kind_idx = schema.index_of("rowkind").unwrap();
        let seq_idx = schema.index_of(SEQUENCE_NUMBER_FIELD_NAME).unwrap();
        let id_idx = schema.index_of("id").unwrap();
        let value_idx = schema.index_of("value").unwrap();
        let kinds = batch
            .column(kind_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seqs = batch
            .column(seq_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                kinds.value(row).to_string(),
                seqs.value(row),
                ids.value(row),
                values.value(row),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn audit_log_changelog_scan_exposes_rowkind_as_first_column() {
    let table_path = "memory:/audit_log/changelog_rowkind";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(
            vec![1, 1, 2, 2],
            vec![10, 20, 25, 30],
            vec![0, 1, 2, 3],
        ))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Changelog, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();

    assert_eq!(batches[0].schema().field(0).name(), "rowkind");
    assert_eq!(
        collect_audit_rows(&batches),
        vec![
            ("+I".to_string(), 1, 10),
            ("+U".to_string(), 2, 25),
            ("-D".to_string(), 2, 30),
            ("-U".to_string(), 1, 20),
        ]
    );
}

#[tokio::test]
async fn audit_log_delta_scan_emits_plus_i_for_all_rows() {
    let table_path = "memory:/audit_log/delta_plus_i";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(table_path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;

    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();

    let rowkind_field = batches[0].schema().field(0).clone();
    assert_eq!(rowkind_field.name(), ROW_KIND_FIELD_NAME);
    assert_eq!(rowkind_field.data_type(), &arrow_schema::DataType::Utf8);
    assert!(rowkind_field.is_nullable());
    assert_eq!(
        rowkind_field.metadata().get("PARQUET:field_id"),
        Some(&ROW_KIND_FIELD_ID.to_string())
    );

    assert_eq!(
        collect_audit_rows(&batches),
        vec![("+I".to_string(), 1, 10), ("+I".to_string(), 2, 20),]
    );
}

#[tokio::test]
async fn audit_log_delta_scan_preserves_pk_row_kinds() {
    let table_path = "memory:/audit_log/delta_rowkind";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(
            // Use distinct keys so the PK writer does not merge multiple
            // changes for one key before the audit read sees the data file.
            vec![1, 2, 3, 4],
            vec![10, 20, 25, 30],
            vec![0, 1, 2, 3],
        ))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let audit = AuditLogTable::new(table.clone());
    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();

    assert_eq!(
        collect_audit_rows(&batches),
        vec![
            ("+I".to_string(), 1, 10),
            ("+U".to_string(), 3, 25),
            ("-D".to_string(), 4, 30),
            ("-U".to_string(), 2, 20),
        ]
    );
}

#[test]
fn audit_log_rowkind_field_matches_java_special_field() {
    let (_, table) = memory_table("memory:/audit_log/rowkind_field", pk_schema(&[]));
    let field = AuditLogTable::new(table).fields().unwrap().remove(0);

    assert_eq!(field.id(), ROW_KIND_FIELD_ID);
    assert_eq!(field.name(), ROW_KIND_FIELD_NAME);
    assert!(matches!(
        field.data_type(),
        DataType::VarChar(varchar) if varchar.length() == VarCharType::MAX_LENGTH
    ));
}

#[tokio::test]
async fn audit_log_exposes_sequence_number_when_enabled() {
    let table_path = "memory:/audit_log/sequence_number";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
            ("table-read.sequence-number.enabled", "true"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(vec![1, 1], vec![10, 20], vec![0, 1]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let audit = AuditLogTable::new(table.clone());
    let field_names: Vec<String> = audit
        .fields()
        .unwrap()
        .into_iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(
        field_names,
        vec![
            "rowkind".to_string(),
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            "id".to_string(),
            "value".to_string(),
        ]
    );

    let plan = audit
        .new_incremental_scan(IncrementalScanMode::Changelog, 0, 1)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = audit.to_arrow(&plan).unwrap().try_collect().await.unwrap();
    let batch_schema: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        batch_schema,
        vec![
            "rowkind".to_string(),
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            "id".to_string(),
            "value".to_string(),
        ]
    );

    let rows = collect_audit_rows_with_sequence(&batches);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, seq, _, _)| *seq >= 0));
}

#[tokio::test]
async fn audit_log_diff_mode_is_unsupported() {
    let table_path = "memory:/audit_log/diff_unsupported";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;

    let audit = AuditLogTable::new(table.clone());
    let err = audit
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::Unsupported { .. }),
        "expected Unsupported for Diff audit plan, got {err:?}"
    );
}
