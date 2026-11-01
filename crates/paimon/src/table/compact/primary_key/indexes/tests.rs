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
use crate::io::FileIOBuilder;
use crate::spec::{
    ArrayType, DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema, VarCharType,
};
use crate::table::global_index_scanner::GlobalIndexScanner;
use crate::table::RowRange;
use arrow_array::{
    builder::{Int32Builder, ListBuilder},
    ArrayRef, Int32Array, Int8Array, RecordBatch, StringArray,
};
use std::sync::Arc;

/// Read the actual native payload instead of relying on a query's raw fallback.
#[tokio::test]
async fn scalar_payloads_address_nulls_and_tombstones_by_physical_ordinal() {
    for kind in ["btree", "bitmap", "multivalue", "fm"] {
        let data_type = match kind {
            "fm" => DataType::VarChar(VarCharType::string_type()),
            "multivalue" => DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            _ => DataType::Int(IntType::new()),
        };
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", data_type)
            .primary_key(["id"])
            .option("bucket", "1")
            .option("write-only", "true")
            .option("index-file-in-data-file-dir", "false")
            .option(
                "fields.value.pk-fm.index.options",
                r#"{"sa-sample-rate":1}"#,
            )
            .option(format!("pk-{kind}.index.columns"), "value")
            .build()
            .unwrap();
        let location = format!("memory:/pk-index-native/{}", uuid::Uuid::new_v4());
        let io = FileIOBuilder::new("memory").build().unwrap();
        let table = Table::new(
            io.clone(),
            crate::catalog::Identifier::new("default", "native"),
            location.clone(),
            TableSchema::new(0, &schema),
            None,
        );
        io.mkdirs(&format!("{location}/schema")).await.unwrap();
        io.mkdirs(&format!("{location}/snapshot")).await.unwrap();
        io.new_output(&format!("{location}/schema/schema-0"))
            .unwrap()
            .write(bytes::Bytes::from(
                serde_json::to_vec(table.schema()).unwrap(),
            ))
            .await
            .unwrap();
        let values: ArrayRef = match kind {
            "fm" => Arc::new(StringArray::from(vec![Some("ten"), None, Some("thirty")])),
            "multivalue" => {
                let mut builder = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(
                    arrow_schema::Field::new("element", arrow_schema::DataType::Int32, true),
                ));
                builder.values().append_value(10);
                builder.append(true);
                builder.append(false);
                builder.values().append_slice(&[30, 30]);
                builder.append(true);
                Arc::new(builder.finish())
            }
            _ => Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
        };
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            ("value", values),
            (
                "_VALUE_KIND",
                Arc::new(Int8Array::from(vec![0, 0, 3])) as ArrayRef,
            ),
        ])
        .unwrap();
        let write = table.new_write_builder();
        let mut writer = write.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        write
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let compact = table.new_compact_builder();
        let plan = compact.new_planner().unwrap().plan().await.unwrap();
        let result = compact
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap();
        assert_eq!(result.files()[0].delete_row_count, Some(1));
        let entries = result.index_entries().unwrap();
        assert_eq!(entries.len(), 1);
        let scanner = GlobalIndexScanner::create_with_fm_options(
            &io,
            &location,
            1,
            i64::MAX,
            128 * 1024 * 1024,
            i64::MAX,
            entries,
            table.schema().fields(),
            crate::fm_index::FMReadOptions {
                locate_cost_ratio: 1.0,
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
        let predicates = PredicateBuilder::new(table.schema().fields());
        let predicate = match kind {
            "fm" => predicates
                .contains("value", Datum::String("irty".into()))
                .unwrap(),
            "multivalue" => predicates.array_contains("value", Datum::Int(30)).unwrap(),
            _ => predicates.equal("value", Datum::Int(30)).unwrap(),
        };
        assert_eq!(
            scanner.matching_ranges(&predicate).await.unwrap(),
            Some(vec![RowRange::new(2, 2)]),
            "{kind}"
        );
        if matches!(kind, "btree" | "bitmap") {
            assert_eq!(
                scanner
                    .matching_ranges(&predicates.is_null("value").unwrap())
                    .await
                    .unwrap(),
                Some(vec![RowRange::new(1, 1)]),
                "{kind}"
            );
        }
    }
}
