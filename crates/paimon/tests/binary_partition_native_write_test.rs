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

use arrow_array::{ArrayRef, BinaryArray, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarBinaryType};
use std::sync::Arc;

async fn check_binary_partition(primary_key: bool) {
    let path = if primary_key {
        "memory:/binary_partition/pk"
    } else {
        "memory:/binary_partition/append"
    };
    let mut schema = Schema::builder()
        .column("bin", DataType::VarBinary(VarBinaryType::new(64).unwrap()))
        .column("id", DataType::Int(IntType::new()))
        .partition_keys(["bin"])
        .option("bucket", "1")
        .option("partition.legacy-name", "false");
    if primary_key {
        schema = schema.primary_key(["id"]);
    } else {
        schema = schema.option("bucket-key", "id");
    }
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;

    let bin: ArrayRef = Arc::new(BinaryArray::from_iter_values([
        b"a/b".as_slice(),
        b"a=b".as_slice(),
        "\u{00A0}".as_bytes(),
        b"\x1c".as_slice(),
        b"\xED\xA0\x80".as_slice(),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("bin", ArrowDataType::Binary, true),
            ArrowField::new("id", ArrowDataType::Int32, true),
        ])),
        vec![bin, Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 5);
    builder.new_commit().commit(messages).await.unwrap();

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let paths = plan
        .splits()
        .iter()
        .map(|split| split.bucket_path().to_string())
        .collect::<Vec<_>>();
    assert!(
        paths.iter().any(|path| path.contains("bin=a%2Fb/")),
        "{paths:?}"
    );
    assert!(
        paths.iter().any(|path| path.contains("bin=a%3Db/")),
        "{paths:?}"
    );
    // These directory names match Java's Character.isWhitespace and UTF-8 decoder.
    for expected in [
        "bin=\u{00A0}/",
        "bin=__DEFAULT_PARTITION__/",
        "bin=\u{FFFD}/",
    ] {
        assert!(
            paths.iter().any(|path| path.contains(expected)),
            "missing {expected} in {paths:?}"
        );
    }
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn append_binary_partitions_round_trip() {
    check_binary_partition(false).await;
}

#[tokio::test]
async fn primary_key_binary_partitions_round_trip() {
    check_binary_partition(true).await;
}
