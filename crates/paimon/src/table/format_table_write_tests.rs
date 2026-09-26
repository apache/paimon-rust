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

//! Format Table write behavior is tested through the same public builder,
//! scan and read calls used by native callers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Int32Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;

use super::Table;
use crate::catalog::Identifier;
use crate::io::{FileIO, FileIOBuilder};
use crate::spec::{
    BooleanType, DataType, DecimalType, IntType, Schema, TableSchema, VarBinaryType, VarCharType,
};

fn table(io: FileIO, location: &str, partitioned: bool, options: &[(&str, &str)]) -> Table {
    let mut builder = Schema::builder();
    if partitioned {
        builder = builder.column("dt", DataType::VarChar(VarCharType::string_type()));
    }
    builder = builder
        .column("id", DataType::Int(IntType::new()))
        .option("type", "format-table")
        .option("file.format", "parquet");
    if partitioned {
        builder = builder.partition_keys(["dt"]);
    }
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    Table::new(
        io,
        Identifier::new("default", "format_write"),
        location.to_string(),
        TableSchema::new(0, &builder.build().unwrap()),
        None,
    )
}

fn memory_table(name: &str, partitioned: bool, options: &[(&str, &str)]) -> Table {
    table(
        FileIOBuilder::new("memory").build().unwrap(),
        &format!("memory:/{name}"),
        partitioned,
        options,
    )
}

fn two_partition_table(name: &str) -> Table {
    let schema = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::string_type()))
        .column("hour", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .partition_keys(["dt", "hour"])
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_two_partition"),
        format!("memory:/{name}"),
        TableSchema::new(0, &schema),
        None,
    )
}

fn two_partition_batch(rows: &[(&str, &str, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("dt", ArrowType::Utf8, true),
            Field::new("hour", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn batch(rows: &[(&str, i32)]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("dt", ArrowType::Utf8, true),
        Field::new("id", ArrowType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn unpartitioned_batch(ids: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowType::Int32,
            true,
        )])),
        vec![Arc::new(Int32Array::from(ids.to_vec()))],
    )
    .unwrap()
}

async fn ids(table: &Table) -> Vec<i32> {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        let index = batch.schema().index_of("id").unwrap();
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        ids.extend(column.iter().flatten());
    }
    ids.sort_unstable();
    ids
}

async fn count_with_empty_projection(table: &Table) -> usize {
    let mut builder = table.new_read_builder();
    builder.with_projection(&[]).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

async fn visible_files(table: &Table, partition: &str) -> Vec<String> {
    let path = if partition.is_empty() {
        table.location().to_string()
    } else {
        format!("{}/{partition}", table.location())
    };
    let mut files =
        super::format_table_scan::list_format_table_files(table.file_io(), &path, 0, None)
            .await
            .unwrap()
            .into_iter()
            .filter(|status| !status.is_dir)
            .map(|status| status.path)
            .collect::<Vec<_>>();
    files.sort();
    files
}

async fn append(table: &Table, batch: &RecordBatch) {
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(batch).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

#[tokio::test]
async fn append_partitioned_table_writes_data_without_partition_column() {
    let table = memory_table("format_append_partitioned", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&batch(&[("a", 1), ("b", 2), ("a", 3)]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 2);
    for message in &messages {
        let file = message.format_file.as_ref().unwrap();
        assert!(file.staged_path.contains("/_temporary/"));
        assert!(!table.file_io().exists(&file.target_path).await.unwrap());
        assert_eq!(file.partition.len(), 1);
    }
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(ids(&table).await, [1, 2, 3]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=b").await.len(), 1);
}

#[tokio::test]
async fn append_unpartitioned_table_has_no_snapshot() {
    let table = memory_table("format_append_plain", false, &[]);
    append(&table, &unpartitioned_batch(&[4, 5])).await;
    append(&table, &unpartitioned_batch(&[6])).await;
    assert_eq!(ids(&table).await, [4, 5, 6]);
    assert_eq!(visible_files(&table, "").await.len(), 2);
    assert!(table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn format_table_ignores_paimon_bucket_configuration() {
    let table = memory_table("format_bucket_ignored", true, &[("bucket", "2")]);
    append(&table, &batch(&[("a", 1)])).await;
    assert_eq!(ids(&table).await, [1]);
}

#[tokio::test]
async fn declared_column_default_routes_null_partition_to_default_directory() {
    let table = memory_table("format_column_default", true, &[]);
    let mut schema = serde_json::to_value(table.schema()).unwrap();
    schema["fields"][0]["defaultValue"] = serde_json::json!("'fallback'");
    let schema = serde_json::from_value(schema).unwrap();
    let table = Table::new(
        table.file_io().clone(),
        table.identifier().clone(),
        table.location().to_string(),
        schema,
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("dt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![None, Some("given")])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
        ],
    )
    .unwrap();
    append(&table, &batch).await;
    assert_eq!(ids(&table).await, [1, 2]);
    assert_eq!(visible_files(&table, "dt=fallback").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=given").await.len(), 1);
}

#[tokio::test]
async fn file_rolls_at_target_row_count_even_inside_one_batch() {
    let table = memory_table("format_roll_rows", true, &[("target-file-row-num", "2")]);
    append(
        &table,
        &batch(&[("a", 1), ("a", 2), ("a", 3), ("a", 4), ("a", 5)]),
    )
    .await;
    assert_eq!(ids(&table).await, [1, 2, 3, 4, 5]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 3);
}

#[tokio::test]
async fn overwrite_replaces_only_written_partitions_by_default() {
    let table = memory_table("format_overwrite_dynamic", true, &[]);
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 7)])).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder
        .new_commit()
        .overwrite(messages, None)
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [2, 7]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 1);
}

#[tokio::test]
async fn static_partition_overwrite_without_output_clears_partition() {
    let table = memory_table("format_overwrite_empty", true, &[]);
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    let builder = table.new_write_builder().with_overwrite();
    builder
        .new_commit()
        .overwrite(
            Vec::new(),
            Some(HashMap::from([(
                "dt".to_string(),
                Some(crate::spec::Datum::String("a".into())),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [2]);
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn prepared_files_are_hidden_until_commit_and_abort_discards_them() {
    let table = memory_table("format_abort_prepared", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&batch(&[("a", 1), ("a", 2)]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    let staged = &messages[0].format_file.as_ref().unwrap().staged_path;
    assert!(table.file_io().exists(staged).await.unwrap());
    assert!(visible_files(&table, "dt=a").await.is_empty());
    assert!(table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()
        .is_empty());
    builder.new_commit().abort(&messages).await.unwrap();
    assert!(!table.file_io().exists(staged).await.unwrap());
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn closing_a_writer_discards_pending_data_without_publishing() {
    let table = memory_table("format_close_pending", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 1)])).await.unwrap();
    write.close().await;
    assert!(visible_files(&table, "dt=a").await.is_empty());
    assert!(table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()
        .is_empty());
}

#[tokio::test]
async fn rejected_batch_does_not_stage_or_publish_a_file() {
    let table = memory_table("format_rejected_batch", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    let wrong = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("dt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["invalid"])),
        ],
    )
    .unwrap();
    let error = write.write_arrow_batch(&wrong).await.err().unwrap();
    assert!(error.to_string().contains("expects"));
    assert!(write.prepare_commit().await.is_err());
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn one_writer_can_prepare_more_than_one_append() {
    let table = memory_table("format_writer_reuse", true, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 1)])).await.unwrap();
    builder
        .new_commit()
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();
    write
        .write_arrow_batch(&batch(&[("a", 2), ("b", 3)]))
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [1, 2, 3]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 2);
    assert_eq!(visible_files(&table, "dt=b").await.len(), 1);
}

#[tokio::test]
async fn value_only_partition_path_is_written_and_read() {
    let table = memory_table(
        "format_value_only_write",
        true,
        &[("format-table.partition-path-only-value", "true")],
    );
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    assert_eq!(ids(&table).await, [1, 2]);
    assert_eq!(visible_files(&table, "a").await.len(), 1);
    assert_eq!(visible_files(&table, "b").await.len(), 1);
    assert!(visible_files(&table, "dt=a").await.is_empty());
}

#[tokio::test]
async fn overwrite_without_dynamic_partition_mode_replaces_all_partitions() {
    let table = memory_table(
        "format_overwrite_all",
        true,
        &[("dynamic-partition-overwrite", "false")],
    );
    append(&table, &batch(&[("a", 1), ("b", 2)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 7)])).await.unwrap();
    builder
        .new_commit()
        .overwrite(write.prepare_commit().await.unwrap(), None)
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [7]);
    assert!(visible_files(&table, "dt=b").await.is_empty());
}

#[tokio::test]
async fn target_collision_is_rejected_before_any_overwrite_cleanup() {
    let table = memory_table("format_target_collision", true, &[]);
    append(&table, &batch(&[("a", 1)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 2)])).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let file = messages[0].format_file.as_ref().unwrap();
    table
        .file_io()
        .copy_file_streaming(&file.staged_path, &file.target_path)
        .await
        .unwrap();
    let old_files = visible_files(&table, "dt=a").await;
    let error = builder
        .new_commit()
        .overwrite(messages.clone(), None)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("target already exists"));
    assert_eq!(visible_files(&table, "dt=a").await, old_files);
    builder.new_commit().abort(&messages).await.unwrap();
    assert_eq!(visible_files(&table, "dt=a").await, old_files);
}

#[tokio::test]
async fn missing_staged_file_cannot_erase_an_overwritten_partition() {
    let table = memory_table("format_missing_stage", true, &[]);
    append(&table, &batch(&[("a", 1)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch(&[("a", 2)])).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let staged = &messages[0].format_file.as_ref().unwrap().staged_path;
    table.file_io().delete_file(staged).await.unwrap();
    let error = builder
        .new_commit()
        .overwrite(messages, None)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("is missing"));
    assert_eq!(ids(&table).await, [1]);
}

#[tokio::test]
async fn format_file_message_refuses_snapshot_wire_serialization() {
    let table = memory_table("format_message_serialize", false, &[]);
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&unpartitioned_batch(&[1]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let error = messages[0].serialize().err().unwrap();
    assert!(error.to_string().contains("different Java serializer"));
    builder.new_commit().abort(&messages).await.unwrap();
}

#[tokio::test]
async fn data_file_name_honors_prefix_and_compression_suffix() {
    let table = memory_table(
        "format_file_name",
        true,
        &[
            ("data-file.prefix", "event-"),
            ("file.suffix.include.compression", "true"),
        ],
    );
    append(&table, &batch(&[("a", 1)])).await;
    let files = visible_files(&table, "dt=a").await;
    assert_eq!(files.len(), 1);
    let name = files[0].rsplit('/').next().unwrap();
    assert!(name.starts_with("event-"), "{name}");
    assert!(name.ends_with(".snappy.parquet"), "{name}");
    assert_eq!(ids(&table).await, [1]);
}

#[tokio::test]
async fn static_prefix_overwrite_removes_only_matching_descendants() {
    let table = two_partition_table("format_static_prefix");
    append(
        &table,
        &two_partition_batch(&[("a", "00", 1), ("a", "01", 2), ("b", "00", 3)]),
    )
    .await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&two_partition_batch(&[("a", "01", 7)]))
        .await
        .unwrap();
    builder
        .new_commit()
        .overwrite(
            write.prepare_commit().await.unwrap(),
            Some(HashMap::from([(
                "dt".to_string(),
                Some(crate::spec::Datum::String("a".into())),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [3, 7]);
    assert!(visible_files(&table, "dt=a/hour=00").await.is_empty());
    assert_eq!(visible_files(&table, "dt=a/hour=01").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=b/hour=00").await.len(), 1);
}

#[tokio::test]
async fn empty_static_overwrite_creates_the_selected_partition_directory() {
    let table = two_partition_table("format_static_empty");
    let prefix = HashMap::from([
        (
            "dt".to_string(),
            Some(crate::spec::Datum::String("new".into())),
        ),
        (
            "hour".to_string(),
            Some(crate::spec::Datum::String("09".into())),
        ),
    ]);
    table
        .new_write_builder()
        .with_overwrite()
        .new_commit()
        .overwrite(Vec::new(), Some(prefix))
        .await
        .unwrap();
    assert!(table
        .file_io()
        .exists_dir("memory:/format_static_empty/dt=new/hour=09")
        .await
        .unwrap());
    assert!(ids(&table).await.is_empty());
}

#[tokio::test]
async fn output_outside_static_prefix_is_rejected_before_deleting_old_files() {
    let table = two_partition_table("format_static_mismatch");
    append(&table, &two_partition_batch(&[("a", "00", 1)])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&two_partition_batch(&[("b", "00", 2)]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    let error = builder
        .new_commit()
        .overwrite(
            messages.clone(),
            Some(HashMap::from([(
                "dt".to_string(),
                Some(crate::spec::Datum::String("a".into())),
            )])),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("outside the static overwrite"));
    assert_eq!(ids(&table).await, [1]);
    builder.new_commit().abort(&messages).await.unwrap();
}

#[tokio::test]
async fn static_partition_must_be_a_leading_prefix() {
    let table = two_partition_table("format_static_gap");
    let error = table
        .new_write_builder()
        .with_overwrite()
        .new_commit()
        .overwrite(
            Vec::new(),
            Some(HashMap::from([(
                "hour".to_string(),
                Some(crate::spec::Datum::String("00".into())),
            )])),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("lacks its leading partition"));
    assert!(ids(&table).await.is_empty());
}

#[tokio::test]
async fn physical_parquet_file_omits_partition_columns() {
    let table = two_partition_table("format_physical_columns");
    append(&table, &two_partition_batch(&[("a", "00", 7)])).await;
    let files = visible_files(&table, "dt=a/hour=00").await;
    assert_eq!(files.len(), 1);
    let bytes = table
        .file_io()
        .new_input(&files[0])
        .unwrap()
        .read()
        .await
        .unwrap();
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let fields = reader.schema().fields();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "id");
    assert_eq!(ids(&table).await, [7]);
}

#[tokio::test]
async fn row_format_round_trips_through_native_read() {
    for format in ["row"] {
        let name = format!("format_write_{format}");
        let table = memory_table(&name, true, &[("file.format", format)]);
        append(&table, &batch(&[("a", 1), ("b", 2), ("a", 3)])).await;
        assert_eq!(ids(&table).await, [1, 2, 3], "format={format}");
        assert_eq!(
            count_with_empty_projection(&table).await,
            3,
            "format={format}"
        );
        assert_eq!(
            visible_files(&table, "dt=a")
                .await
                .iter()
                .filter(|path| path.ends_with(&format!(".{format}")))
                .count(),
            1,
            "format={format}"
        );
    }
}

#[tokio::test]
async fn null_partition_uses_the_configured_default_directory() {
    let table = memory_table(
        "format_null_partition",
        true,
        &[("partition.default-name", "_empty_")],
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("dt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>, Some("a")])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    append(&table, &batch).await;
    assert_eq!(ids(&table).await, [1, 2]);
    assert_eq!(visible_files(&table, "dt=_empty_").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 1);
}

#[test]
fn invalid_target_row_count_is_rejected_before_a_writer_is_opened() {
    let table = memory_table(
        "format_bad_row_target",
        false,
        &[("target-file-row-num", "0")],
    );
    let error = table.new_write_builder().new_write().err().unwrap();
    assert!(error
        .to_string()
        .contains("target-file-row-num must be positive"));
}

#[test]
fn unsupported_format_is_rejected_before_a_writer_is_opened() {
    let table = memory_table(
        "format_unsupported_write",
        false,
        &[("file.format", "unknown")],
    );
    let error = table.new_write_builder().new_write().err().unwrap();
    assert!(error.to_string().contains("not supported"));
}

#[tokio::test]
async fn java_format_table_formats_round_trip() {
    for format in ["parquet", "orc", "csv", "json", "mosaic"] {
        let table = memory_table(
            &format!("format_{format}_round_trip"),
            true,
            &[("file.format", format)],
        );
        append(&table, &batch(&[("a", 1), ("a", 2), ("b", 3)])).await;
        assert_eq!(ids(&table).await, [1, 2, 3], "format={format}");
        assert_eq!(
            visible_files(&table, "dt=a").await.len(),
            1,
            "format={format}"
        );
        assert_eq!(
            visible_files(&table, "dt=b").await.len(),
            1,
            "format={format}"
        );
    }
}

#[tokio::test]
async fn csv_header_round_trips_and_java_json_string_numbers_are_readable() {
    let csv = memory_table(
        "format_csv_header",
        false,
        &[("file.format", "csv"), ("csv.include-header", "true")],
    );
    append(&csv, &unpartitioned_batch(&[7, 8])).await;
    let path = visible_files(&csv, "").await.remove(0);
    let contents = csv
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(&contents[..], b"id\n7\n8\n");
    assert_eq!(ids(&csv).await, [7, 8]);

    let json = memory_table("format_java_json", false, &[("file.format", "json")]);
    json.file_io()
        .new_output(&format!("{}/part-0.json", json.location()))
        .unwrap()
        .write(Bytes::from_static(b"{\"id\":\"7\"}\n{\"id\":\"8\"}\n"))
        .await
        .unwrap();
    assert_eq!(ids(&json).await, [7, 8]);
}

#[tokio::test]
async fn csv_and_json_binary_fields_use_java_base64_encoding() {
    for format in ["csv", "json"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "payload",
                DataType::VarBinary(VarBinaryType::new(32).unwrap()),
            )
            .option("type", "format-table")
            .option("file.format", format)
            .build()
            .unwrap();
        let location = format!("memory:/format_{format}_binary");
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "format_binary"),
            location,
            TableSchema::new(0, &schema),
            None,
        );
        let input = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", ArrowType::Int32, true),
                Field::new("payload", ArrowType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(BinaryArray::from(vec![Some(&b"Hi"[..])])),
            ],
        )
        .unwrap();
        append(&table, &input).await;
        let path = visible_files(&table, "").await.remove(0);
        let contents = table
            .file_io()
            .new_input(&path)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&contents).contains("SGk="),
            "format={format}"
        );
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let payload = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(payload.value(0), b"Hi", "format={format}");
    }
}

#[tokio::test]
async fn json_binary_conversion_preserves_decimal_precision() {
    let schema = Schema::builder()
        .column(
            "amount",
            DataType::Decimal(DecimalType::new(38, 4).unwrap()),
        )
        .column(
            "payload",
            DataType::VarBinary(VarBinaryType::new(32).unwrap()),
        )
        .option("type", "format-table")
        .option("file.format", "json")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_json_decimal_binary"),
        "memory:/format_json_decimal_binary".into(),
        TableSchema::new(0, &schema),
        None,
    );
    let unscaled: i128 = "12345678901234567890123456789012345678".parse().unwrap();
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("amount", ArrowType::Decimal128(38, 4), true),
            Field::new("payload", ArrowType::Binary, true),
        ])),
        vec![
            Arc::new(
                Decimal128Array::from(vec![Some(unscaled)])
                    .with_precision_and_scale(38, 4)
                    .unwrap(),
            ),
            Arc::new(BinaryArray::from(vec![Some(&b"Hi"[..])])),
        ],
    )
    .unwrap();
    append(&table, &input).await;
    let path = visible_files(&table, "").await.remove(0);
    let written = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&written).contains("1234567890123456789012345678901234.5678"));
    table
        .file_io()
        .new_output(&format!("{}/external.json", table.location()))
        .unwrap()
        .write(Bytes::from_static(
            b"{\"amount\":1234567890123456789012345678901234.5678,\"payload\":\"SGk=\"}\n",
        ))
        .await
        .unwrap();
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, [unscaled, unscaled]);
}

#[tokio::test]
async fn java_json_boolean_strings_are_readable() {
    let schema = Schema::builder()
        .column("flag", DataType::Boolean(BooleanType::new()))
        .option("type", "format-table")
        .option("file.format", "json")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_java_json_boolean"),
        "memory:/format_java_json_boolean".into(),
        TableSchema::new(0, &schema),
        None,
    );
    table
        .file_io()
        .new_output(&format!("{}/java.json", table.location()))
        .unwrap()
        .write(Bytes::from_static(
            b"{\"flag\":\"true\"}\n{\"flag\":\"false\"}\n",
        ))
        .await
        .unwrap();
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let flags = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(flags.iter().collect::<Vec<_>>(), [Some(true), Some(false)]);

    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "flag",
            ArrowType::Boolean,
            true,
        )])),
        vec![Arc::new(BooleanArray::from(vec![Some(true)]))],
    )
    .unwrap();
    append(&table, &input).await;
    let path = visible_files(&table, "")
        .await
        .into_iter()
        .find(|path| !path.ends_with("/java.json"))
        .unwrap();
    let written = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&written).contains("\"flag\":\"true\""));
}

#[tokio::test]
async fn csv_reads_java_doubled_and_backslash_quoted_fields() {
    let schema = Schema::builder()
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .option("type", "format-table")
        .option("file.format", "csv")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_java_csv_quotes"),
        "memory:/format_java_csv_quotes".into(),
        TableSchema::new(0, &schema),
        None,
    );
    table
        .file_io()
        .new_output(&format!("{}/java.csv", table.location()))
        .unwrap()
        .write(Bytes::from_static(b"\"a\"\"b\"\n\"c\\\"d\"\n"))
        .await
        .unwrap();
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some("a\"b"), Some("c\"d")]
    );
}

#[tokio::test]
async fn csv_preserves_quoted_empty_string_separately_from_null() {
    let schema = Schema::builder()
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .option("type", "format-table")
        .option("file.format", "csv")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_csv_empty"),
        "memory:/format_csv_empty".into(),
        TableSchema::new(0, &schema),
        None,
    );
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            ArrowType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(vec![Some(""), None]))],
    )
    .unwrap();
    append(&table, &input).await;
    let path = visible_files(&table, "").await.remove(0);
    let bytes = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"\"\"\n\n");
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "");
    assert!(!values.is_null(0));
    assert!(values.is_null(1));
}

#[tokio::test]
async fn csv_custom_delimiters_keep_line_breaks_and_partial_row_separator() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .option("type", "format-table")
        .option("file.format", "csv")
        .option("csv.field-delimiter", ";")
        .option("csv.line-delimiter", "||")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_csv_custom"),
        "memory:/format_csv_custom".into(),
        TableSchema::new(0, &schema),
        None,
    );
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a|", "line\nbreak"])),
        ],
    )
    .unwrap();
    append(&table, &input).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "a|");
    assert_eq!(values.value(1), "line\nbreak");
}

#[tokio::test]
async fn text_format_table_round_trip() {
    let schema = Schema::builder()
        .column("line", DataType::VarChar(VarCharType::string_type()))
        .option("type", "format-table")
        .option("file.format", "text")
        .build()
        .unwrap();
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "format_text_round_trip"),
        "memory:/format_text_round_trip".into(),
        TableSchema::new(0, &schema),
        None,
    );
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "line",
            ArrowType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(vec!["first", "second"]))],
    )
    .unwrap();
    append(&table, &input).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|index| array.value(index).to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, ["first", "second"]);
    assert_eq!(count_with_empty_projection(&table).await, 2);
}

#[tokio::test]
async fn compressed_text_format_tables_round_trip_with_java_file_suffixes() {
    let codecs = [
        ("gzip", "gz"),
        ("bzip2", "bz2"),
        ("deflate", "deflate"),
        ("snappy", "snappy"),
        ("lz4", "lz4"),
        ("zstd", "zst"),
    ];
    for format in ["csv", "json", "text"] {
        for (codec, suffix) in codecs {
            let mut builder = Schema::builder()
                .column(
                    if format == "text" { "line" } else { "id" },
                    if format == "text" {
                        DataType::VarChar(VarCharType::string_type())
                    } else {
                        DataType::Int(IntType::new())
                    },
                )
                .option("type", "format-table")
                .option("file.format", format)
                .option("file.compression", codec);
            if format == "csv" && codec == "gzip" {
                builder = builder
                    .option("file.suffix.include.compression", "true")
                    .option("csv.include-header", "true");
            }
            let schema = builder.build().unwrap();
            let location = format!("memory:/format_{format}_{codec}_compressed");
            let table = Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "format_compressed_text"),
                location,
                TableSchema::new(0, &schema),
                None,
            );
            let input = if format == "text" {
                RecordBatch::try_new(
                    Arc::new(ArrowSchema::new(vec![Field::new(
                        "line",
                        ArrowType::Utf8,
                        true,
                    )])),
                    vec![Arc::new(StringArray::from(vec!["first", "second"]))],
                )
                .unwrap()
            } else {
                unpartitioned_batch(&[7, 8])
            };
            let builder = table.new_write_builder();
            let mut write = builder.new_write().unwrap();
            write.write_arrow_batch(&input.slice(0, 1)).await.unwrap();
            write.write_arrow_batch(&input.slice(1, 1)).await.unwrap();
            let messages = write.prepare_commit().await.unwrap();
            builder.new_commit().commit(messages).await.unwrap();
            let files = visible_files(&table, "").await;
            assert_eq!(files.len(), 1, "{format}/{codec}");
            assert!(
                files[0].ends_with(&format!(".{format}.{suffix}")),
                "{}",
                files[0]
            );
            let content = table
                .file_io()
                .new_input(&files[0])
                .unwrap()
                .read()
                .await
                .unwrap();
            assert!(!content.is_empty(), "{format}/{codec}");
            if codec == "gzip" {
                assert!(content.starts_with(&[0x1f, 0x8b]));
            } else if codec == "bzip2" {
                assert!(content.starts_with(b"BZh"));
            } else if codec == "zstd" {
                assert!(content.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]));
            } else if matches!(codec, "snappy" | "lz4") {
                let block_len = u32::from_be_bytes(content[..4].try_into().unwrap()) as usize;
                if format == "text" {
                    assert_eq!(block_len, b"first\n".len());
                } else if format == "csv" {
                    assert_eq!(block_len, b"7\n".len());
                } else {
                    assert!(block_len > 0);
                }
            }
            if format == "text" {
                let plan = table.new_read_builder().new_scan().plan().await.unwrap();
                let batches: Vec<RecordBatch> = table
                    .new_read_builder()
                    .new_read()
                    .unwrap()
                    .to_arrow(plan.splits())
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let values = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .flatten()
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(values, ["first", "second"], "{codec}");
            } else {
                assert_eq!(ids(&table).await, [7, 8], "{format}/{codec}");
            }
            assert_eq!(
                count_with_empty_projection(&table).await,
                2,
                "{format}/{codec}"
            );
        }
    }
}

#[tokio::test]
async fn compressed_csv_is_detected_from_path_even_without_table_compression_option() {
    use std::io::Write;

    let table = memory_table("format_external_gzip_csv", false, &[("file.format", "csv")]);
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"7\n8\n").unwrap();
    let compressed = encoder.finish().unwrap();
    table
        .file_io()
        .new_output(&format!("{}/external.csv.gz", table.location()))
        .unwrap()
        .write(Bytes::from(compressed))
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [7, 8]);
}

#[tokio::test]
async fn bzip2_csv_is_detected_from_path_even_without_table_compression_option() {
    use std::io::Write;

    let table = memory_table(
        "format_external_bzip2_csv",
        false,
        &[("file.format", "csv")],
    );
    let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
    encoder.write_all(b"7\n8\n").unwrap();
    let compressed = encoder.finish().unwrap();
    table
        .file_io()
        .new_output(&format!("{}/external.csv.bz2", table.location()))
        .unwrap()
        .write(Bytes::from(compressed))
        .await
        .unwrap();
    assert_eq!(ids(&table).await, [7, 8]);
}

#[tokio::test]
async fn avro_format_table_append_is_readable_through_native_scan() {
    let table = memory_table("format_avro_append", true, &[("file.format", "avro")]);
    append(&table, &batch(&[("a", 1), ("a", 2), ("b", 3)])).await;
    assert_eq!(ids(&table).await, [1, 2, 3]);
    assert_eq!(visible_files(&table, "dt=a").await.len(), 1);
    assert_eq!(visible_files(&table, "dt=b").await.len(), 1);
}
