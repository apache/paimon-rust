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

use std::sync::{Arc, OnceLock};

use crate::spec::{
    BinaryRow, DataField, DeletionVectorMeta, FileKind, IndexManifest, IndexManifestEntry,
};
use crate::table::Table;
use arrow_array::builder::{
    ArrayBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

use super::row_string_cast::format_row_as_java_cast_string;

use crate::Result;

pub(super) fn table_indexes_schema() -> SchemaRef {
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

pub(super) async fn read(
    table: &Table,
    _projection: Option<&[usize]>,
    _predicate: Option<&crate::spec::Predicate>,
) -> Result<RecordBatch> {
    let entries = collect_index_entries(table).await?;

    let partition_fields = table.schema().partition_fields();
    let fields = table.schema().fields();
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
        file_sizes.push(index_file.file_size);
        row_counts.push(index_file.row_count);
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
            index_field_names.push(global_index_field_names(global_meta, fields));
        } else {
            row_range_starts.push(None);
            row_range_ends.push(None);
            index_field_ids.push(None);
            index_field_names.push(None);
        }
    }

    let schema = table_indexes_schema();
    let batch = super::record_batch(
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

    Ok(batch)
}

async fn collect_index_entries(table: &Table) -> crate::Result<Vec<IndexManifestEntry>> {
    let file_io = table.file_io();
    let manifest_sm =
        crate::table::SnapshotManager::new(file_io.clone(), table.location().to_string());
    let Some(snapshot) = table.resolve_read_snapshot().await? else {
        return Ok(Vec::new());
    };
    let Some(index_manifest_name) = snapshot.index_manifest() else {
        return Ok(Vec::new());
    };

    let path = manifest_sm.manifest_path(index_manifest_name);
    if !file_io.exists(&path).await? {
        return Ok(Vec::new());
    }

    let entries = IndexManifest::read(file_io, &path).await?;
    Ok(visible_index_entries(entries))
}

fn visible_index_entries(entries: Vec<IndexManifestEntry>) -> Vec<IndexManifestEntry> {
    entries
        .into_iter()
        .filter(|entry| entry.kind == FileKind::Add)
        .collect()
}

fn global_index_field_names(
    meta: &crate::spec::GlobalIndexMeta,
    fields: &[DataField],
) -> Option<String> {
    let names: Option<Vec<_>> = std::iter::once(&meta.index_field_id)
        .chain(meta.extra_field_ids.iter().flatten())
        .map(|id| {
            fields
                .iter()
                .find(|field| field.id() == *id)
                .map(DataField::name)
        })
        .collect();
    names.map(|names| names.join(","))
}

fn format_partition(partition: &[u8], partition_fields: &[DataField]) -> Result<String> {
    let row = BinaryRow::from_serialized_bytes(partition)?;
    format_row_as_java_cast_string(&row, partition_fields)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::IndexFileMeta;

    fn index_entry(kind: FileKind, file_name: &str) -> IndexManifestEntry {
        IndexManifestEntry {
            kind,
            partition: vec![],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "DELETION_VECTORS".to_string(),
                file_name: file_name.to_string(),
                file_size: 1,
                row_count: 1,
                deletion_vectors_ranges: None,
                external_path: None,
                global_index_meta: None,
            },
            version: 1,
        }
    }

    #[test]
    fn composite_index_names_preserve_order_and_require_every_field() {
        use crate::spec::{GlobalIndexMeta, IntType};
        let fields = vec![
            DataField::new(3, "a".into(), crate::spec::DataType::Int(IntType::new())),
            DataField::new(
                7,
                "renamed".into(),
                crate::spec::DataType::Int(IntType::new()),
            ),
        ];
        let mut meta = GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: 1,
            index_field_id: 7,
            extra_field_ids: Some(vec![3]),
            index_meta: None,
            source_meta: None,
        };
        assert_eq!(
            global_index_field_names(&meta, &fields).as_deref(),
            Some("renamed,a")
        );
        meta.extra_field_ids = Some(vec![99]);
        assert_eq!(global_index_field_names(&meta, &fields), None);
        meta.extra_field_ids = None;
        assert_eq!(
            global_index_field_names(&meta, &fields).as_deref(),
            Some("renamed")
        );
    }

    #[test]
    fn visible_index_entries_skips_delete_entries() {
        let entries = vec![
            index_entry(FileKind::Add, "live.idx"),
            index_entry(FileKind::Delete, "deleted.idx"),
        ];

        let visible = visible_index_entries(entries);

        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].kind, FileKind::Add);
        assert_eq!(visible[0].index_file.file_name, "live.idx");
    }
}
