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

//! Mirrors Java [ManifestsTable](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/table/system/ManifestsTable.java).

use std::any::Any;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::spec::{BinaryRow, DataField, ManifestFileMeta, ManifestList};
use paimon::table::{SnapshotManager, Table};

use super::row_string_cast::format_row_as_java_cast_string;
use crate::error::to_datafusion_error;

const MIN_PARTITION_STATS_INDEX: usize = 5;
const MAX_PARTITION_STATS_INDEX: usize = 6;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(ManifestsTable { table }))
}

fn manifests_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("file_name", DataType::Utf8, false),
                Field::new("file_size", DataType::Int64, false),
                Field::new("num_added_files", DataType::Int64, false),
                Field::new("num_deleted_files", DataType::Int64, false),
                Field::new("schema_id", DataType::Int64, false),
                Field::new("min_partition_stats", DataType::Utf8, true),
                Field::new("max_partition_stats", DataType::Utf8, true),
                Field::new("min_row_id", DataType::Int64, true),
                Field::new("max_row_id", DataType::Int64, true),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct ManifestsTable {
    table: Table,
}

#[async_trait]
impl TableProvider for ManifestsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        manifests_schema()
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
        let metas =
            crate::runtime::await_with_runtime(async move { collect_manifests(&table).await })
                .await
                .map_err(to_datafusion_error)?;

        let n = metas.len();
        let mut file_names: Vec<String> = Vec::with_capacity(n);
        let mut file_sizes = Vec::with_capacity(n);
        let mut num_added = Vec::with_capacity(n);
        let mut num_deleted = Vec::with_capacity(n);
        let mut schema_ids = Vec::with_capacity(n);
        let mut min_partition_stats: Vec<Option<String>> = Vec::with_capacity(n);
        let mut max_partition_stats: Vec<Option<String>> = Vec::with_capacity(n);
        let mut min_row_ids: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut max_row_ids: Vec<Option<i64>> = Vec::with_capacity(n);
        let partition_fields = self.table.schema().partition_fields();
        let projected_columns = projection.map(Vec::as_slice);
        let materialize_min_partition_stats =
            should_materialize_column(projected_columns, MIN_PARTITION_STATS_INDEX);
        let materialize_max_partition_stats =
            should_materialize_column(projected_columns, MAX_PARTITION_STATS_INDEX);

        for meta in metas {
            let stats = meta.partition_stats();
            file_names.push(meta.file_name().to_string());
            file_sizes.push(meta.file_size());
            num_added.push(meta.num_added_files());
            num_deleted.push(meta.num_deleted_files());
            schema_ids.push(meta.schema_id());
            min_partition_stats.push(
                materialize_partition_stats_value(
                    materialize_min_partition_stats,
                    stats.min_values(),
                    stats.null_counts(),
                    &partition_fields,
                )
                .map_err(to_datafusion_error)?,
            );
            max_partition_stats.push(
                materialize_partition_stats_value(
                    materialize_max_partition_stats,
                    stats.max_values(),
                    stats.null_counts(),
                    &partition_fields,
                )
                .map_err(to_datafusion_error)?,
            );
            min_row_ids.push(meta.min_row_id());
            max_row_ids.push(meta.max_row_id());
        }

        let schema = manifests_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(file_names)),
                Arc::new(Int64Array::from(file_sizes)),
                Arc::new(Int64Array::from(num_added)),
                Arc::new(Int64Array::from(num_deleted)),
                Arc::new(Int64Array::from(schema_ids)),
                Arc::new(StringArray::from(min_partition_stats)),
                Arc::new(StringArray::from(max_partition_stats)),
                Arc::new(Int64Array::from(min_row_ids)),
                Arc::new(Int64Array::from(max_row_ids)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

async fn collect_manifests(table: &Table) -> paimon::Result<Vec<ManifestFileMeta>> {
    let file_io = table.file_io();
    let sm = SnapshotManager::new(file_io.clone(), table.location().to_string());
    let snapshot = match sm.get_latest_snapshot().await? {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };

    let base_path = sm.manifest_path(snapshot.base_manifest_list());
    let delta_path = sm.manifest_path(snapshot.delta_manifest_list());
    let changelog_path = snapshot
        .changelog_manifest_list()
        .map(|c| sm.manifest_path(c));
    let base_fut = ManifestList::read(file_io, &base_path);
    let delta_fut = ManifestList::read(file_io, &delta_path);
    let changelog_fut = async {
        match &changelog_path {
            Some(p) => ManifestList::read(file_io, p).await,
            None => Ok(Vec::new()),
        }
    };
    let (base, delta, changelog) = futures::try_join!(base_fut, delta_fut, changelog_fut)?;
    let mut metas = base;
    metas.extend(delta);
    metas.extend(changelog);
    Ok(metas)
}

fn should_materialize_column(projection: Option<&[usize]>, column_index: usize) -> bool {
    match projection {
        Some(projection) => projection.contains(&column_index),
        None => true,
    }
}

fn materialize_partition_stats_value(
    materialize: bool,
    value_bytes: &[u8],
    null_counts: &[Option<i64>],
    partition_fields: &[DataField],
) -> paimon::Result<Option<String>> {
    if materialize {
        format_partition_stats_value(value_bytes, null_counts, partition_fields)
    } else {
        Ok(None)
    }
}

fn format_partition_stats_value(
    value_bytes: &[u8],
    null_counts: &[Option<i64>],
    partition_fields: &[DataField],
) -> paimon::Result<Option<String>> {
    if value_bytes.is_empty() {
        return if partition_fields.is_empty() || null_counts.len() == partition_fields.len() {
            Ok(Some(format_all_null_partition_row(partition_fields.len())))
        } else {
            Ok(None)
        };
    }

    let row = BinaryRow::from_serialized_bytes(value_bytes)?;
    format_row_as_java_cast_string(&row, partition_fields).map(Some)
}

fn format_all_null_partition_row(arity: usize) -> String {
    if arity == 0 {
        return "{}".to_string();
    }
    format!("{{{}}}", vec!["null"; arity].join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::spec::{DataType as PaimonDataType, Datum, FloatType, IntType, VarCharType};

    fn field(name: &str, data_type: PaimonDataType) -> DataField {
        DataField::new(0, name.to_string(), data_type)
    }

    fn serialized_row(values: &[(Option<Datum>, PaimonDataType)]) -> Vec<u8> {
        let refs: Vec<_> = values
            .iter()
            .map(|(datum, data_type)| (datum.as_ref(), data_type))
            .collect();
        BinaryRow::from_datums(&refs).to_serialized_bytes()
    }

    #[test]
    fn test_should_materialize_column() {
        let projected_stats = vec![MIN_PARTITION_STATS_INDEX];
        let projected_without_stats = vec![0, 1, 2];

        assert!(should_materialize_column(None, MIN_PARTITION_STATS_INDEX));
        assert!(should_materialize_column(
            Some(projected_stats.as_slice()),
            MIN_PARTITION_STATS_INDEX
        ));
        assert!(!should_materialize_column(
            Some(projected_without_stats.as_slice()),
            MIN_PARTITION_STATS_INDEX
        ));
    }

    #[test]
    fn test_unprojected_partition_stats_are_not_formatted() {
        let data_type = PaimonDataType::Float(FloatType::new());
        let fields = vec![field("pt", data_type.clone())];
        let bytes = serialized_row(&[(Some(Datum::Float(1.0)), data_type.clone())]);

        assert_eq!(
            materialize_partition_stats_value(false, &bytes, &[Some(0)], &fields).unwrap(),
            None
        );
        assert_eq!(
            materialize_partition_stats_value(true, &bytes, &[Some(0)], &fields).unwrap(),
            Some("{1.0}".to_string())
        );
    }

    #[test]
    fn test_format_empty_partition_row() {
        assert_eq!(
            format_partition_stats_value(&[], &[], &[]).unwrap(),
            Some("{}".to_string())
        );
    }

    #[test]
    fn test_format_empty_bytes_with_matching_null_counts_as_all_null() {
        let fields = vec![
            field("pt1", PaimonDataType::Int(IntType::new())),
            field("pt2", PaimonDataType::VarChar(VarCharType::string_type())),
        ];
        assert_eq!(
            format_partition_stats_value(&[], &[Some(2), Some(2)], &fields).unwrap(),
            Some("{null, null}".to_string())
        );
    }

    #[test]
    fn test_format_empty_bytes_with_mismatched_null_counts_as_unknown() {
        let fields = vec![field("pt", PaimonDataType::Int(IntType::new()))];
        assert_eq!(
            format_partition_stats_value(&[], &[], &fields).unwrap(),
            None
        );
    }
}
