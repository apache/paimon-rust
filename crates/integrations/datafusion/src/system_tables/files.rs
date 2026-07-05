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

//! Mirrors Java [FilesTable](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/table/system/FilesTable.java).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{
    Int32Array, Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::spec::{BinaryRow, DataField, DataFileMeta, TableSchema};
use paimon::table::{DataSplit, Table};

use super::row_string_cast::{
    format_row_as_java_array_string, format_row_as_java_cast_string,
    format_row_field_as_java_cast_string,
};
use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(FilesTable { table }))
}

fn files_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("partition", ArrowDataType::Utf8, true),
                Field::new("bucket", ArrowDataType::Int32, false),
                Field::new("file_path", ArrowDataType::Utf8, false),
                Field::new("file_format", ArrowDataType::Utf8, false),
                Field::new("schema_id", ArrowDataType::Int64, false),
                Field::new("level", ArrowDataType::Int32, false),
                Field::new("record_count", ArrowDataType::Int64, false),
                Field::new("file_size_in_bytes", ArrowDataType::Int64, false),
                Field::new("min_key", ArrowDataType::Utf8, true),
                Field::new("max_key", ArrowDataType::Utf8, true),
                Field::new("null_value_counts", ArrowDataType::Utf8, false),
                Field::new("min_value_stats", ArrowDataType::Utf8, false),
                Field::new("max_value_stats", ArrowDataType::Utf8, false),
                Field::new("min_sequence_number", ArrowDataType::Int64, true),
                Field::new("max_sequence_number", ArrowDataType::Int64, true),
                Field::new(
                    "creation_time",
                    ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("delete_row_count", ArrowDataType::Int64, true),
                Field::new("file_source", ArrowDataType::Utf8, true),
                Field::new("first_row_id", ArrowDataType::Int64, true),
                Field::new(
                    "write_cols",
                    ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true))),
                    true,
                ),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct FilesTable {
    table: Table,
}

#[async_trait]
impl TableProvider for FilesTable {
    fn schema(&self) -> SchemaRef {
        files_schema()
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
        let rows =
            crate::runtime::await_with_runtime(async move { collect_file_rows(&table).await })
                .await
                .map_err(to_datafusion_error)?;

        let schema = files_schema();
        let batch = file_rows_to_record_batch(&rows)?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

#[derive(Debug)]
struct FileRow {
    partition: Option<String>,
    bucket: i32,
    file_path: String,
    file_format: String,
    schema_id: i64,
    level: i32,
    record_count: i64,
    file_size_in_bytes: i64,
    min_key: Option<String>,
    max_key: Option<String>,
    null_value_counts: String,
    min_value_stats: String,
    max_value_stats: String,
    min_sequence_number: Option<i64>,
    max_sequence_number: Option<i64>,
    creation_time: Option<i64>,
    delete_row_count: Option<i64>,
    file_source: Option<String>,
    first_row_id: Option<i64>,
    write_cols: Option<Vec<String>>,
}

async fn collect_file_rows(table: &Table) -> paimon::Result<Vec<FileRow>> {
    let scan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await?;
    let partition_fields = table.schema().partition_fields();
    let mut schema_cache = HashMap::new();
    schema_cache.insert(table.schema().id(), Arc::new(table.schema().clone()));

    let mut rows = Vec::new();
    for split in scan.splits() {
        rows.extend(data_split_rows(table, split, &partition_fields, &mut schema_cache).await?);
    }
    Ok(rows)
}

async fn data_split_rows(
    table: &Table,
    split: &DataSplit,
    partition_fields: &[DataField],
    schema_cache: &mut HashMap<i64, Arc<TableSchema>>,
) -> paimon::Result<Vec<FileRow>> {
    let partition = format_partition(split.partition(), partition_fields)?;
    let mut rows = Vec::with_capacity(split.data_files().len());
    for file in split.data_files() {
        let file_schema = schema_for_id(table, schema_cache, file.schema_id).await?;
        let key_fields = key_fields_for_schema(&file_schema);
        let min_key = format_key(&file.min_key, &key_fields)?;
        let max_key = format_key(&file.max_key, &key_fields)?;
        let stats = format_value_stats(table, schema_cache, file).await?;

        rows.push(FileRow {
            partition: partition.clone(),
            bucket: split.bucket(),
            file_path: file
                .external_path
                .clone()
                .unwrap_or_else(|| split.data_file_path(file)),
            file_format: data_file_format_identifier(&file.file_name)?,
            schema_id: file.schema_id,
            level: file.level,
            record_count: file.row_count,
            file_size_in_bytes: file.file_size,
            min_key,
            max_key,
            null_value_counts: stats.null_value_counts,
            min_value_stats: stats.min_value_stats,
            max_value_stats: stats.max_value_stats,
            min_sequence_number: Some(file.min_sequence_number),
            max_sequence_number: Some(file.max_sequence_number),
            creation_time: file.creation_time.map(|t| t.timestamp_millis()),
            delete_row_count: file.delete_row_count,
            file_source: file_source_to_string(file.file_source),
            first_row_id: file.first_row_id,
            write_cols: file.write_cols.clone(),
        });
    }
    Ok(rows)
}

async fn schema_for_id(
    table: &Table,
    schema_cache: &mut HashMap<i64, Arc<TableSchema>>,
    schema_id: i64,
) -> paimon::Result<Arc<TableSchema>> {
    if let Some(schema) = schema_cache.get(&schema_id) {
        return Ok(schema.clone());
    }
    let schema = table.schema_manager().schema(schema_id).await?;
    schema_cache.insert(schema_id, schema.clone());
    Ok(schema)
}

fn format_partition(
    partition: &BinaryRow,
    partition_fields: &[DataField],
) -> paimon::Result<Option<String>> {
    if partition_fields.is_empty() {
        return Ok(Some("{}".to_string()));
    }
    format_row_as_java_cast_string(partition, partition_fields).map(Some)
}

fn key_fields_for_schema(schema: &TableSchema) -> Vec<DataField> {
    let trimmed_primary_keys = schema.trimmed_primary_keys();
    if trimmed_primary_keys.is_empty() {
        return schema.fields().to_vec();
    }

    trimmed_primary_keys
        .iter()
        .filter_map(|name| {
            schema
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .cloned()
        })
        .collect()
}

fn format_key(bytes: &[u8], key_fields: &[DataField]) -> paimon::Result<Option<String>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let row = BinaryRow::from_serialized_bytes(bytes)?;
    if row.arity() <= 0 {
        return Ok(None);
    }
    format_row_as_java_array_string(&row, key_fields).map(Some)
}

#[derive(Debug)]
struct FormattedStats {
    null_value_counts: String,
    min_value_stats: String,
    max_value_stats: String,
}

async fn format_value_stats(
    table: &Table,
    schema_cache: &mut HashMap<i64, Arc<TableSchema>>,
    file: &DataFileMeta,
) -> paimon::Result<FormattedStats> {
    let table_fields = table.schema().fields();
    let file_schema = schema_for_id(table, schema_cache, file.schema_id).await?;
    let data_fields = file_schema.fields();
    let field_mapping = table_to_data_field_mapping(table_fields, data_fields);
    let dense_mapping = dense_stats_mapping(data_fields, file.value_stats_cols.as_deref());
    let min_row = decode_stats_row(file.value_stats.min_values())?;
    let max_row = decode_stats_row(file.value_stats.max_values())?;

    let mut null_counts = BTreeMap::new();
    let mut lower_bounds = BTreeMap::new();
    let mut upper_bounds = BTreeMap::new();

    for (table_index, table_field) in table_fields.iter().enumerate() {
        let data_index = field_mapping.get(table_index).copied().flatten();
        let stats_index =
            data_index.and_then(|idx| stats_index_for_data_field(idx, &dense_mapping));

        let null_count = match (data_index, stats_index) {
            (None, _) => Some(file.row_count),
            (Some(_), Some(idx)) => file.value_stats.null_counts().get(idx).copied().flatten(),
            (Some(_), None) => None,
        };
        null_counts.insert(table_field.name().to_string(), null_count);

        let value_type = data_index
            .and_then(|idx| data_fields.get(idx))
            .map(DataField::data_type)
            .unwrap_or_else(|| table_field.data_type());
        lower_bounds.insert(
            table_field.name().to_string(),
            format_stats_value(min_row.as_ref(), stats_index, value_type)?,
        );
        upper_bounds.insert(
            table_field.name().to_string(),
            format_stats_value(max_row.as_ref(), stats_index, value_type)?,
        );
    }

    Ok(FormattedStats {
        null_value_counts: format_java_map(&null_counts, |v| {
            v.map(|count| count.to_string())
                .unwrap_or_else(|| "null".to_string())
        }),
        min_value_stats: format_java_map(&lower_bounds, |v| {
            v.clone().unwrap_or_else(|| "null".to_string())
        }),
        max_value_stats: format_java_map(&upper_bounds, |v| {
            v.clone().unwrap_or_else(|| "null".to_string())
        }),
    })
}

fn table_to_data_field_mapping(
    table_fields: &[DataField],
    data_fields: &[DataField],
) -> Vec<Option<usize>> {
    let data_field_index: HashMap<i32, usize> = data_fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.id(), idx))
        .collect();
    let mapping: Vec<Option<usize>> = table_fields
        .iter()
        .map(|field| data_field_index.get(&field.id()).copied())
        .collect();

    let identity = mapping.len() == data_fields.len()
        && mapping
            .iter()
            .enumerate()
            .all(|(idx, mapped)| *mapped == Some(idx));
    if identity {
        (0..table_fields.len()).map(Some).collect()
    } else {
        mapping
    }
}

fn dense_stats_mapping(
    data_fields: &[DataField],
    dense_fields: Option<&[String]>,
) -> Option<Vec<Option<usize>>> {
    dense_fields.map(|dense_fields| {
        let dense_index: HashMap<&str, usize> = dense_fields
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.as_str(), idx))
            .collect();
        data_fields
            .iter()
            .map(|field| dense_index.get(field.name()).copied())
            .collect()
    })
}

fn stats_index_for_data_field(
    data_index: usize,
    dense_mapping: &Option<Vec<Option<usize>>>,
) -> Option<usize> {
    match dense_mapping {
        None => Some(data_index),
        Some(mapping) => mapping.get(data_index).copied().flatten(),
    }
}

fn decode_stats_row(bytes: &[u8]) -> paimon::Result<Option<BinaryRow>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        BinaryRow::from_serialized_bytes(bytes).map(Some)
    }
}

fn format_stats_value(
    row: Option<&BinaryRow>,
    stats_index: Option<usize>,
    data_type: &paimon::spec::DataType,
) -> paimon::Result<Option<String>> {
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(stats_index) = stats_index else {
        return Ok(None);
    };
    if stats_index >= row.arity() as usize {
        return Ok(None);
    }
    format_row_field_as_java_cast_string(row, stats_index, data_type)
}

fn format_java_map<T, F>(map: &BTreeMap<String, T>, value_to_string: F) -> String
where
    F: Fn(&T) -> String,
{
    let mut out = String::from("{");
    for (idx, (key, value)) in map.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        out.push_str(key);
        out.push('=');
        out.push_str(&value_to_string(value));
    }
    out.push('}');
    out
}

fn data_file_format_identifier(file_name: &str) -> paimon::Result<String> {
    let Some(dot) = file_name.rfind('.') else {
        return Err(paimon::Error::DataInvalid {
            message: format!("{file_name} is not a legal file name."),
            source: None,
        });
    };

    let extension = &file_name[dot + 1..];
    if is_hadoop_compression_extension(extension) {
        let Some(second_dot) = file_name[..dot].rfind('.') else {
            return Err(paimon::Error::DataInvalid {
                message: format!("{file_name} is not a legal file name."),
                source: None,
            });
        };
        return Ok(file_name[second_dot + 1..dot].to_string());
    }

    Ok(extension.to_string())
}

fn is_hadoop_compression_extension(extension: &str) -> bool {
    ["gz", "bz2", "deflate", "snappy", "lz4", "zst"]
        .iter()
        .any(|known| extension.eq_ignore_ascii_case(known))
}

fn file_source_to_string(file_source: Option<i32>) -> Option<String> {
    file_source.map(|source| match source {
        0 => "APPEND".to_string(),
        1 => "COMPACT".to_string(),
        other => other.to_string(),
    })
}

fn file_rows_to_record_batch(rows: &[FileRow]) -> DFResult<RecordBatch> {
    let n = rows.len();
    let mut partitions = Vec::with_capacity(n);
    let mut buckets = Vec::with_capacity(n);
    let mut file_paths = Vec::with_capacity(n);
    let mut file_formats = Vec::with_capacity(n);
    let mut schema_ids = Vec::with_capacity(n);
    let mut levels = Vec::with_capacity(n);
    let mut record_counts = Vec::with_capacity(n);
    let mut file_sizes = Vec::with_capacity(n);
    let mut min_keys = Vec::with_capacity(n);
    let mut max_keys = Vec::with_capacity(n);
    let mut null_value_counts = Vec::with_capacity(n);
    let mut min_value_stats = Vec::with_capacity(n);
    let mut max_value_stats = Vec::with_capacity(n);
    let mut min_sequence_numbers = Vec::with_capacity(n);
    let mut max_sequence_numbers = Vec::with_capacity(n);
    let mut creation_times = Vec::with_capacity(n);
    let mut delete_row_counts = Vec::with_capacity(n);
    let mut file_sources = Vec::with_capacity(n);
    let mut first_row_ids = Vec::with_capacity(n);
    let mut write_cols = ListBuilder::new(StringBuilder::new());

    for row in rows {
        partitions.push(row.partition.clone());
        buckets.push(row.bucket);
        file_paths.push(row.file_path.clone());
        file_formats.push(row.file_format.clone());
        schema_ids.push(row.schema_id);
        levels.push(row.level);
        record_counts.push(row.record_count);
        file_sizes.push(row.file_size_in_bytes);
        min_keys.push(row.min_key.clone());
        max_keys.push(row.max_key.clone());
        null_value_counts.push(row.null_value_counts.clone());
        min_value_stats.push(row.min_value_stats.clone());
        max_value_stats.push(row.max_value_stats.clone());
        min_sequence_numbers.push(row.min_sequence_number);
        max_sequence_numbers.push(row.max_sequence_number);
        creation_times.push(row.creation_time);
        delete_row_counts.push(row.delete_row_count);
        file_sources.push(row.file_source.clone());
        first_row_ids.push(row.first_row_id);
        match &row.write_cols {
            Some(cols) => {
                for col in cols {
                    write_cols.values().append_value(col);
                }
                write_cols.append(true);
            }
            None => write_cols.append(false),
        }
    }

    Ok(RecordBatch::try_new(
        files_schema(),
        vec![
            Arc::new(StringArray::from(partitions)),
            Arc::new(Int32Array::from(buckets)),
            Arc::new(StringArray::from(file_paths)),
            Arc::new(StringArray::from(file_formats)),
            Arc::new(Int64Array::from(schema_ids)),
            Arc::new(Int32Array::from(levels)),
            Arc::new(Int64Array::from(record_counts)),
            Arc::new(Int64Array::from(file_sizes)),
            Arc::new(StringArray::from(min_keys)),
            Arc::new(StringArray::from(max_keys)),
            Arc::new(StringArray::from(null_value_counts)),
            Arc::new(StringArray::from(min_value_stats)),
            Arc::new(StringArray::from(max_value_stats)),
            Arc::new(Int64Array::from(min_sequence_numbers)),
            Arc::new(Int64Array::from(max_sequence_numbers)),
            Arc::new(TimestampMillisecondArray::from(creation_times)),
            Arc::new(Int64Array::from(delete_row_counts)),
            Arc::new(StringArray::from(file_sources)),
            Arc::new(Int64Array::from(first_row_ids)),
            Arc::new(write_cols.finish()),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_format_identifier() {
        assert_eq!(
            data_file_format_identifier("part-0.parquet").unwrap(),
            "parquet"
        );
        assert_eq!(data_file_format_identifier("part-0.csv.gz").unwrap(), "csv");
        assert_eq!(
            data_file_format_identifier("part-0.orc.zst").unwrap(),
            "orc"
        );
    }

    #[test]
    fn test_format_java_map() {
        let mut map = BTreeMap::new();
        map.insert("b".to_string(), Some(2));
        map.insert("a".to_string(), None);
        assert_eq!(
            format_java_map(&map, |v| v
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string())),
            "{a=null, b=2}"
        );
    }
}
