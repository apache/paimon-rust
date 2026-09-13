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

use crate::catalog::Identifier;
use crate::io::{FileIO, FileIOBuilder};
use crate::spec::{
    ArrayType, DataType, Datum, FloatType, IntType, Predicate, PredicateBuilder, Schema,
    TableSchema,
};
use crate::table::{Table, TableCommit, TableWrite};
use crate::vindex::IVF_FLAT_IDENTIFIER;
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) fn vector_test_table() -> Table {
    vector_test_table_at("memory:/vector_test")
}

pub(super) fn vector_test_table_at(location: &str) -> Table {
    vector_test_table_with_file_io(FileIOBuilder::new("memory").build().unwrap(), location)
}

pub(super) fn vector_test_table_with_file_io(file_io: FileIO, location: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .build()
        .unwrap();
    Table::new(
        file_io,
        Identifier::new("default", "vector_test"),
        location.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

/// Build a real vindex IVF-flat segment trained with `metric`, returning the
/// serialized bytes. `nlist = 1` keeps training trivial and deterministic; the
/// only thing the metric check cares about is the persisted metadata metric.
pub(super) fn build_vindex_segment_bytes(metric: &str) -> Vec<u8> {
    use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
    use paimon_vindex_core::io::PosWriter;

    const DIM: usize = 2;
    let vectors: Vec<f32> = vec![1.0, 0.0, 0.0, 1.0, 1.0, 1.0];
    let n = vectors.len() / DIM;
    let ids: Vec<i64> = (0..n as i64).collect();
    let options = HashMap::from([
        ("index.type".to_string(), "ivf_flat".to_string()),
        ("dimension".to_string(), DIM.to_string()),
        ("nlist".to_string(), "1".to_string()),
        ("metric".to_string(), metric.to_string()),
    ]);
    let config = VectorIndexConfig::from_options(&options).unwrap();
    let training = VectorIndexTrainer::train(config, &vectors, n).unwrap();
    let mut writer = VectorIndexWriter::new(training);
    writer.add_vectors(&ids, &vectors, n).unwrap();
    let mut bytes = Vec::new();
    {
        let mut output = PosWriter::new(&mut bytes);
        writer.write(&mut output).unwrap();
    }
    bytes
}

pub(super) fn pk_vector_table(options: &[(&str, &str)]) -> Table {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        );
    if options
        .iter()
        .any(|(key, _)| *key == "pk-vector.index.columns")
    {
        builder = builder.primary_key(["id"]).option("bucket", "1");
    }
    let schema = builder.build().unwrap();
    // Runtime validation must remain defensive for schemas committed by old
    // or external writers, including malformed configurations which the
    // current Schema builder rejects at commit time.
    let runtime_options = options
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    let table_schema = TableSchema::new(0, &schema).copy_with_options(runtime_options);
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "pk_vector_test"),
        "memory:/pk_vector_test".to_string(),
        table_schema,
        None,
    )
}

/// A data-evolution (global-index) vector table with a committed IVF-flat
/// index over the `embedding` column: row-tracking + data-evolution +
/// global-index enabled so committed data files carry `first_row_id` and the
/// search returns global row-ids that `execute_read` can materialize. The
/// returned table has one committed batch of `(id, embedding)` rows and a real
/// vindex index built end-to-end.
pub(super) async fn de_vector_table() -> Table {
    let table_path = "memory:/de_vector_search_test";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("global-index.enabled", "true")
        .option("global-index.row-count-per-shard", "10")
        .option("ivf-flat.dimension", "2")
        .option("ivf-flat.nlist", "2")
        .build()
        .unwrap();
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "de_vector_test"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    );
    file_io
        .mkdirs(&format!("{table_path}/snapshot/"))
        .await
        .unwrap();
    file_io
        .mkdirs(&format!("{table_path}/manifest/"))
        .await
        .unwrap();

    let ids = vec![1, 2, 3];
    let vectors = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]];
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut vector_builder =
        ListBuilder::new(Float32Builder::new()).with_field(element_field.clone());
    for vector in vectors {
        for value in vector {
            vector_builder.values().append_value(value);
        }
        vector_builder.append(true);
    }
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("embedding", ArrowDataType::List(element_field), true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(vector_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap();

    let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
    table_write.write_arrow_batch(&batch).await.unwrap();
    let messages = table_write.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();

    let built = table
        .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
        .with_index_column("embedding")
        .execute()
        .await
        .unwrap();
    assert!(built > 0, "DE fixture must build a global vector index");
    let built = table
        .new_sorted_global_index_build_builder()
        .with_index_column("id")
        .with_index_type("btree")
        .execute()
        .await
        .unwrap();
    assert!(built > 0, "DE fixture must build a scalar BTree index");
    table
}

/// `id > threshold` built against the table's user fields (leaf index resolves
/// against `table.schema().fields()`).
pub(super) fn id_gt_filter(table: &Table, threshold: i32) -> Predicate {
    PredicateBuilder::new(table.schema().fields())
        .greater_than("id", Datum::Int(threshold))
        .unwrap()
}

/// A PK-vector table whose user schema carries an extra column named
/// `reserved`, used to prove reserved metadata names are rejected even when
/// they arrive via the default (all-columns) projection.
pub(super) fn pk_vector_table_with_extra_column(reserved: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .column(reserved, DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("deletion-vectors.enabled", "true")
        .option("pk-vector.index.columns", "embedding")
        .option("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER)
        .option("fields.embedding.pk-vector.distance.metric", "l2")
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "reserved_col_test"),
        "memory:/reserved_col_test".to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}
