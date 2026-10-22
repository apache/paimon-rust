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

//! Verify that active global indexes still describe the indexed column values.

use std::collections::{HashMap, HashSet};

use super::global_index_types::normalize_queryable_global_index_type;
use super::key_normalization::canonical_partition_key;
use super::Table;
use crate::spec::{
    BinaryRow, DataEvolutionIndexSourceMeta, DataField, GlobalIndexColumnUpdateAction,
    IndexManifest, Snapshot,
};

/// Under IGNORE, prove that indexed columns have not changed since the index's
/// source snapshot. Other current update policies bypass this check.
/// `partitions` contains canonical logical partition keys.
pub(super) async fn indexed_columns_unchanged(
    table: &Table,
    snapshot: &Snapshot,
    columns: &[String],
    partitions: &HashSet<Vec<u8>>,
) -> crate::Result<bool> {
    if table
        .schema()
        .core_options()
        .global_index_column_update_action()?
        != GlobalIndexColumnUpdateAction::Ignore
    {
        return Ok(true);
    }
    let Some(manifest) = snapshot.index_manifest() else {
        return Ok(true);
    };
    let path = table.snapshot_manager().manifest_path(manifest);
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    let fields = table.schema().fields();
    let partition_fields = table.schema().partition_fields();
    let mut indexes = Vec::new();
    for entry in &entries {
        if entry.kind != crate::spec::FileKind::Add
            || normalize_queryable_global_index_type(&entry.index_file.index_type).is_none()
        {
            continue;
        }
        let Some(meta) = &entry.index_file.global_index_meta else {
            continue;
        };
        let index_partition = if partition_fields.is_empty() {
            Vec::new()
        } else {
            canonical_partition_key(
                &BinaryRow::from_serialized_bytes(&entry.partition)?,
                &partition_fields,
            )?
        };
        if !partition_fields.is_empty() && !partitions.contains(&index_partition) {
            continue;
        }
        if meta.row_range_start < 0 || meta.row_range_end < meta.row_range_start {
            return Ok(false);
        }
        let keys = fields
            .iter()
            .filter(|field| {
                columns.iter().any(|column| column == field.name())
                    && (field.id() == meta.index_field_id
                        || meta
                            .extra_field_ids
                            .as_ref()
                            .is_some_and(|ids| ids.contains(&field.id())))
            })
            .collect::<Vec<_>>();
        if keys.is_empty() {
            continue;
        }
        let Some(source_id) = meta
            .source_meta
            .as_deref()
            .and_then(|bytes| DataEvolutionIndexSourceMeta::deserialize(bytes).ok())
            .map(|meta| meta.scan_snapshot_id())
            .filter(|id| *id <= snapshot.id())
        else {
            return Ok(false);
        };
        indexes.push((entry, index_partition, keys, source_id));
    }
    if indexes.is_empty() {
        return Ok(true);
    }
    // Inspect every active column file before key predicates or indexes can
    // prune away evidence of historical updates. This reads metadata only.
    let unindexed = table
        .copy_with_pinned_snapshot(Some(snapshot))
        .copy_with_options(HashMap::from([(
            "global-index.enabled".into(),
            "false".into(),
        )]));
    let plan = unindexed.new_read_builder().new_scan().plan_all().await?;
    for (entry, index_partition, keys, source_id) in indexes {
        let meta = entry.index_file.global_index_meta.as_ref().unwrap();
        for split in plan.splits() {
            if split.bucket() != entry.bucket
                || (!partition_fields.is_empty()
                    && canonical_partition_key(split.partition(), &partition_fields)?
                        != index_partition)
            {
                continue;
            }
            for file in split.data_files() {
                if file.row_id_range().is_some_and(|(start, end)| {
                    start > meta.row_range_end || end < meta.row_range_start
                }) {
                    continue;
                }
                if file.schema_id != table.schema().id()
                    || !file_columns_unchanged(file, table.schema(), &keys, source_id)
                {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn file_columns_unchanged(
    file: &crate::spec::DataFileMeta,
    schema: &crate::spec::TableSchema,
    keys: &[&DataField],
    source_id: i64,
) -> bool {
    // Java aligns column sequences with the physical schema: omitted write
    // columns and row-tracking metadata do not occupy a sequence slot.
    let fields = schema.data_file_fields(file.write_cols.as_deref());
    if fields
        .iter()
        .map(DataField::id)
        .collect::<HashSet<_>>()
        .len()
        != fields.len()
        || file.write_cols.as_ref().is_some_and(|columns| {
            columns.iter().any(|name| {
                !crate::spec::is_row_tracking_column(name)
                    && name != crate::spec::VALUE_KIND_FIELD_NAME
                    && !schema.fields().iter().any(|field| field.name() == name)
            })
        })
    {
        return false;
    }
    keys.iter().all(|key| {
        let Some(position) = fields.iter().position(|field| field.id() == key.id()) else {
            return true;
        };
        let sequence = file
            .column_max_sequence_numbers
            .as_ref()
            .filter(|sequences| sequences.len() == fields.len())
            .and_then(|sequences| sequences.get(position))
            .copied()
            .filter(|sequence| *sequence > 0 && *sequence <= file.max_sequence_number)
            .unwrap_or(file.max_sequence_number);
        file.row_id_range().is_some() && sequence > 0 && sequence <= source_id
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        DataFileMeta, DataType as PaimonType, IntType, Schema as PaimonSchema, TableSchema,
    };

    #[test]
    fn indexed_columns_use_compacted_column_sequences() {
        let schema = PaimonSchema::builder()
            .column("key", PaimonType::Int(IntType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .column("keep", PaimonType::Int(IntType::new()))
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        let mut file = DataFileMeta {
            file_name: "data.parquet".into(),
            file_size: 128,
            row_count: 3,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 1,
            max_sequence_number: 1,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            first_row_id: Some(0),
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            column_max_sequence_numbers: None,
        };
        file.write_cols = Some(vec!["key".into(), "value".into()]);
        file.column_max_sequence_numbers = Some(vec![1, 3]);
        file.max_sequence_number = 3;
        file.file_source = Some(1);
        let key = &schema.fields()[0];
        assert!(file_columns_unchanged(&file, &schema, &[key], 2));
        file.column_max_sequence_numbers = Some(vec![3, 3]);
        assert!(!file_columns_unchanged(&file, &schema, &[key], 2));
        for sequences in [None, Some(vec![1]), Some(vec![0, 3]), Some(vec![4, 3])] {
            file.column_max_sequence_numbers = sequences;
            assert!(!file_columns_unchanged(&file, &schema, &[key], 2));
        }
        file.max_sequence_number = 1;
        file.column_max_sequence_numbers = None;
        assert!(file_columns_unchanged(&file, &schema, &[key], 2));
        file.max_sequence_number = 3;
        file.write_cols = None;
        file.column_max_sequence_numbers = Some(vec![1, 3, 1]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2));
        file.write_cols = Some(vec!["_ROW_ID".into(), "value".into(), "key".into()]);
        file.column_max_sequence_numbers = Some(vec![3, 1]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2));
        for columns in [
            vec!["key".into(), "key".into()],
            vec!["key".into(), "unknown".into()],
        ] {
            file.write_cols = Some(columns);
            file.column_max_sequence_numbers = Some(vec![1, 1]);
            assert!(!file_columns_unchanged(&file, &schema, &[key], 2));
        }

        let schema = PaimonSchema::builder()
            .column("blob", PaimonType::Blob(crate::spec::BlobType::new()))
            .column("key", PaimonType::Int(IntType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.write-cols-optimization.enabled", "true")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        let key = &schema.fields()[1];
        file.write_cols = None;
        file.column_max_sequence_numbers = Some(vec![1, 3]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2));
        file.write_cols = Some(vec!["blob".into()]);
        file.column_max_sequence_numbers = Some(vec![3]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2));
    }
}
