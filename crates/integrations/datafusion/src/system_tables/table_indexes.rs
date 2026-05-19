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

//! Mirrors Java [TableIndexesTable](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/system/TableIndexesTable.java).

use std::any::Any;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::builder::{
    ArrayBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::spec::{BinaryRow, DataField, DeletionVectorMeta, IndexManifest, IndexManifestEntry};
use paimon::table::{SnapshotManager, Table};

use super::row_string_cast::format_row_as_java_cast_string;
use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(TableIndexesTable { table }))
}

fn table_indexes_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("partition", DataType::Utf8, true),
                Field::new("bucket", DataType::Int32, false),
                Field::new("index_type", DataType::Utf8, false),
                Field::new("file_name", DataType::Utf8, false),
                Field::new("file_size", DataType::Int64, false),
                Field::new("row_count", DataType::Int64, false),
                Field::new("dv_ranges", dv_ranges_data_type(), true),
                Field::new("row_range_start", DataType::Int64, true),
                Field::new("row_range_end", DataType::Int64, true),
                Field::new("index_field_id", DataType::Int32, true),
                Field::new("index_field_name", DataType::Utf8, true),
            ]))
        })
        .clone()
}

fn dv_ranges_data_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(dv_meta_fields()),
        true,
    )))
}

fn dv_meta_fields() -> Fields {
    vec![
        Arc::new(Field::new("f0", DataType::Utf8, false)),
        Arc::new(Field::new("f1", DataType::Int32, false)),
        Arc::new(Field::new("f2", DataType::Int32, false)),
        Arc::new(Field::new("_CARDINALITY", DataType::Int64, true)),
    ]
    .into()
}

#[derive(Debug)]
struct TableIndexesTable {
    table: Table,
}

#[async_trait]
impl TableProvider for TableIndexesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        table_indexes_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.table.clone();
        let entries =
            crate::runtime::await_with_runtime(async move { collect_index_entries(&table).await })
                .await
                .map_err(to_datafusion_error)?;

        let partition_fields = self.table.schema().partition_fields();
        let fields = self.table.schema().fields();
        let n = entries.len();
        let mut partitions: Vec<Option<String>> = Vec::with_capacity(n);
        let mut buckets = Vec::with_capacity(n);
        let mut index_types = Vec::with_capacity(n);
        let mut file_names = Vec::with_capacity(n);
        let mut file_sizes = Vec::with_capacity(n);
        let mut row_counts = Vec::with_capacity(n);
        let mut dv_ranges = dv_ranges_builder();
        let mut row_range_starts: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut row_range_ends: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut index_field_ids: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut index_field_names: Vec<Option<String>> = Vec::with_capacity(n);

        for entry in &entries {
            let index_file = &entry.index_file;
            partitions.push(Some(format_partition(&entry.partition, &partition_fields)?));
            buckets.push(entry.bucket);
            index_types.push(index_file.index_type.as_str());
            file_names.push(index_file.file_name.as_str());
            file_sizes.push(i64::from(index_file.file_size));
            row_counts.push(i64::from(index_file.row_count));
            append_dv_ranges(
                &mut dv_ranges,
                index_file
                    .deletion_vectors_ranges
                    .as_ref()
                    .map(|ranges| ranges.iter()),
            );

            if let Some(global_meta) = &index_file.global_index_meta {
                row_range_starts.push(Some(global_meta.row_range_start));
                row_range_ends.push(Some(global_meta.row_range_end));
                index_field_ids.push(Some(global_meta.index_field_id));
                index_field_names.push(
                    fields
                        .iter()
                        .find(|field| field.id() == global_meta.index_field_id)
                        .map(|field| field.name().to_string()),
                );
            } else {
                row_range_starts.push(None);
                row_range_ends.push(None);
                index_field_ids.push(None);
                index_field_names.push(None);
            }
        }

        let schema = table_indexes_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(partitions)),
                Arc::new(Int32Array::from(buckets)),
                Arc::new(StringArray::from(index_types)),
                Arc::new(StringArray::from(file_names)),
                Arc::new(Int64Array::from(file_sizes)),
                Arc::new(Int64Array::from(row_counts)),
                Arc::new(dv_ranges.finish()) as ArrayRef,
                Arc::new(Int64Array::from(row_range_starts)),
                Arc::new(Int64Array::from(row_range_ends)),
                Arc::new(Int32Array::from(index_field_ids)),
                Arc::new(StringArray::from(index_field_names)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

async fn collect_index_entries(table: &Table) -> paimon::Result<Vec<IndexManifestEntry>> {
    let file_io = table.file_io();
    let sm = SnapshotManager::new(file_io.clone(), table.location().to_string());
    let snapshot = match sm.get_latest_snapshot().await? {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };
    let Some(index_manifest_name) = snapshot.index_manifest() else {
        return Ok(Vec::new());
    };

    let path = sm.manifest_path(index_manifest_name);
    if !file_io.exists(&path).await? {
        return Ok(Vec::new());
    }

    IndexManifest::read(file_io, &path).await
}

fn format_partition(partition: &[u8], partition_fields: &[DataField]) -> DFResult<String> {
    let row = BinaryRow::from_serialized_bytes(partition).map_err(to_datafusion_error)?;
    format_row_as_java_cast_string(&row, partition_fields).map_err(to_datafusion_error)
}

fn dv_ranges_builder() -> ListBuilder<StructBuilder> {
    let fields = dv_meta_fields();
    let element_field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));
    let struct_builder = StructBuilder::new(
        fields,
        vec![
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
        ],
    );
    ListBuilder::new(struct_builder).with_field(element_field)
}

fn append_dv_ranges<'a, I>(builder: &mut ListBuilder<StructBuilder>, ranges: Option<I>)
where
    I: IntoIterator<Item = (&'a String, &'a DeletionVectorMeta)>,
{
    let Some(ranges) = ranges else {
        builder.append(false);
        return;
    };

    for (data_file_name, meta) in ranges {
        let values = builder.values();
        values
            .field_builder::<StringBuilder>(0)
            .expect("dv f0 builder")
            .append_value(data_file_name);
        values
            .field_builder::<Int32Builder>(1)
            .expect("dv f1 builder")
            .append_value(meta.offset);
        values
            .field_builder::<Int32Builder>(2)
            .expect("dv f2 builder")
            .append_value(meta.length);
        let cardinality_builder = values
            .field_builder::<Int64Builder>(3)
            .expect("dv _CARDINALITY builder");
        if let Some(cardinality) = meta.cardinality {
            cardinality_builder.append_value(cardinality);
        } else {
            cardinality_builder.append_null();
        }
        values.append(true);
    }
    builder.append(true);
}
