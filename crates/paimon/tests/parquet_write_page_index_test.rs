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

use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{Field, Schema as ArrowSchema};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use paimon::spec::{ArrayType, DataType, IntType, Schema, TableSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::test]
async fn parquet_page_indexes_follow_write_option_for_data_and_changelog() {
    for setting in [None, Some("true"), Some("false")] {
        for changelog in [false, true] {
            let mut schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column(
                    "items",
                    DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
                );
            if changelog {
                schema = schema
                    .primary_key(["id"])
                    .option("bucket", "1")
                    .option("changelog-producer", "input");
            }
            if let Some(setting) = setting {
                schema = schema.option("parquet.write-page-index.enabled", setting);
            }
            // Read-side pruning is independent of whether the writer emits indexes.
            let schema = schema
                .option("parquet.filter.columnindex.enabled", "false")
                .build()
                .unwrap();
            let path = "memory:/parquet_page_indexes";
            let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
            setup_dirs(&file_io, path).await;
            persist_table_schema(&file_io, path, table.schema()).await;

            let mut items = ListBuilder::new(Int32Builder::new());
            items.values().append_value(10);
            items.values().append_null();
            items.append(true);
            items.append(true);
            items.append(false);
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(items.finish()),
            ];
            let batch = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", columns[0].data_type().clone(), false),
                    Field::new("items", columns[1].data_type().clone(), true),
                ])),
                columns,
            )
            .unwrap();
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].new_files.len(), 1);
            assert_eq!(
                messages[0].new_changelog_files.len(),
                usize::from(changelog)
            );

            for file in messages[0]
                .new_files
                .iter()
                .chain(&messages[0].new_changelog_files)
            {
                let file_path = format!("{path}/bucket-0/{}", file.file_name);
                let bytes = file_io.new_input(&file_path).unwrap().read().await.unwrap();
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
                let enabled = setting != Some("false");
                for row_group in reader.metadata().row_groups() {
                    for column in row_group.columns() {
                        assert_eq!(column.column_index_offset().is_some(), enabled);
                        assert_eq!(column.offset_index_offset().is_some(), enabled);
                        assert!(column.statistics().is_some());
                    }
                }
                let ids = reader
                    .build()
                    .unwrap()
                    .flat_map(|batch| {
                        batch
                            .unwrap()
                            .column_by_name("id")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(ids, vec![1, 2, 3]);
            }
        }
    }
}
