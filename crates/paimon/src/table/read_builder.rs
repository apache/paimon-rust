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

//! ReadBuilder and TableRead for table read API.
//!
//! Reference: [Java ReadBuilder.withProjection](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilder.java)
//! and [TypeUtils.project](https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/utils/TypeUtils.java).

use super::bucket_filter::{extract_predicate_for_keys, split_partition_and_data_predicates};
use super::table_read::TableRead;
use super::{Table, TableScan};
use crate::arrow::filtering::reader_pruning_predicates;
use crate::spec::{CoreOptions, DataField, Predicate};
use crate::table::source::RowRange;
use crate::{Error, Result};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
struct NormalizedFilter {
    partition_predicate: Option<Predicate>,
    data_predicates: Vec<Predicate>,
    bucket_predicate: Option<Predicate>,
}

/// Whether a translated predicate is exact at the table-provider boundary.
///
/// Exact filters are fully enforced by paimon-core scan planning using only
/// partition-owned semantics, without requiring residual filtering above the
/// scan.
fn is_exact_filter_pushdown_for_schema(
    fields: &[DataField],
    partition_keys: &[String],
    filter: &Predicate,
) -> bool {
    if partition_keys.is_empty() {
        return false;
    }

    let (_, data_predicates) =
        split_partition_and_data_predicates(filter.clone(), fields, partition_keys);
    data_predicates.is_empty()
}

pub(super) fn split_scan_predicates(
    table: &Table,
    filter: Predicate,
) -> (Option<Predicate>, Vec<Predicate>) {
    let partition_keys = table.schema().partition_keys();
    if partition_keys.is_empty() {
        (None, filter.split_and())
    } else {
        split_partition_and_data_predicates(filter, table.schema().fields(), partition_keys)
    }
}

fn bucket_predicate(table: &Table, filter: &Predicate) -> Option<Predicate> {
    let core_options = CoreOptions::new(table.schema().options());
    if !core_options.is_default_bucket_function() {
        return None;
    }

    let bucket_keys = core_options.bucket_key().unwrap_or_else(|| {
        if table.schema().trimmed_primary_keys().is_empty() {
            Vec::new()
        } else {
            table.schema().trimmed_primary_keys()
        }
    });
    if bucket_keys.is_empty() {
        return None;
    }

    let has_all_bucket_fields = bucket_keys.iter().all(|key| {
        table
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == key)
    });
    if !has_all_bucket_fields {
        return None;
    }

    extract_predicate_for_keys(filter, table.schema().fields(), &bucket_keys)
}

fn normalize_filter(table: &Table, filter: Predicate) -> NormalizedFilter {
    let (partition_predicate, data_predicates) = split_scan_predicates(table, filter.clone());
    NormalizedFilter {
        partition_predicate,
        data_predicates,
        bucket_predicate: bucket_predicate(table, &filter),
    }
}

/// Builder for table scan and table read (new_scan, new_read).
///
/// Rust keeps a names-based projection API for ergonomics, while aligning the
/// resulting read semantics with Java Paimon's order-preserving projection.
#[derive(Debug, Clone)]
pub struct ReadBuilder<'a> {
    table: &'a Table,
    projected_fields: Option<Vec<String>>,
    filter: NormalizedFilter,
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
}

impl<'a> ReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            projected_fields: None,
            filter: NormalizedFilter::default(),
            limit: None,
            row_ranges: None,
        }
    }

    /// Set column projection by name. Output order follows the caller-specified order.
    /// Unknown or duplicate names cause `new_read()` to fail; an empty list is a valid
    /// zero-column projection.
    pub fn with_projection(&mut self, columns: &[&str]) -> &mut Self {
        self.projected_fields = Some(columns.iter().map(|c| (*c).to_string()).collect());
        self
    }

    /// Set a filter predicate for scan planning and conservative read pruning.
    ///
    /// The predicate should use table schema field indices (as produced by
    /// [`PredicateBuilder`]). During [`TableScan::plan`], partition-only
    /// conjuncts are used for partition pruning and supported data conjuncts
    /// may be used for conservative file-stats pruning.
    ///
    /// Stats pruning is per file. Files with a different `schema_id`,
    /// incompatible stats layout, or inconclusive stats are kept.
    ///
    /// [`TableRead`] may use supported non-partition data predicates only on
    /// the regular Parquet read path for conservative row-group pruning and
    /// native Parquet row filtering. Unsupported predicates, non-Parquet
    /// reads, and data-evolution reads remain residual and should still be
    /// applied by the caller if exact filtering semantics are required.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = normalize_filter(self.table, filter);
        self.try_extract_row_id_ranges();
        self
    }

    /// Whether a translated predicate is exact at the table-provider boundary.
    ///
    /// Exact filters are fully enforced by paimon-core scan planning, without
    /// requiring residual filtering above the scan.
    pub fn is_exact_filter_pushdown(&self, filter: &Predicate) -> bool {
        is_exact_filter_pushdown_for_schema(
            self.table.schema().fields(),
            self.table.schema().partition_keys(),
            filter,
        )
    }

    /// Set row ID ranges `[from, to]` (inclusive) for filtering in data evolution mode.
    pub fn with_row_ranges(&mut self, ranges: Vec<RowRange>) -> &mut Self {
        self.row_ranges = if ranges.is_empty() {
            None
        } else {
            Some(ranges)
        };
        self
    }

    /// Extract `_ROW_ID` predicates from data_predicates into row_ranges.
    /// Only runs when no explicit row_ranges have been set.
    fn try_extract_row_id_ranges(&mut self) {
        if self.row_ranges.is_some() || self.filter.data_predicates.is_empty() {
            return;
        }
        let combined = Predicate::and(self.filter.data_predicates.clone());
        if let Some(ranges) = super::row_id_predicate::extract_row_id_ranges(&combined) {
            self.row_ranges = Some(ranges);
            self.filter.data_predicates = self
                .filter
                .data_predicates
                .iter()
                .filter_map(super::row_id_predicate::remove_row_id_filter)
                .collect();
        }
    }

    /// Push a row-limit hint down to scan planning.
    ///
    /// This allows paimon-core scan planning to generate fewer splits when the
    /// current scan state keeps split-level `merged_row_count()` conservative.
    ///
    /// Note: This method does not guarantee that exactly `limit` rows will be
    /// returned by [`TableRead`]. It is only a pushdown hint for planning.
    /// Callers or query engines are responsible for enforcing the final LIMIT.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Create a table scan. Call [TableScan::plan] to get splits.
    pub fn new_scan(&self) -> TableScan<'a> {
        TableScan::new(
            self.table,
            self.filter.partition_predicate.clone(),
            self.filter.data_predicates.clone(),
            self.filter.bucket_predicate.clone(),
            self.limit,
            self.row_ranges.clone(),
        )
    }

    /// Create a table read for consuming splits (e.g. from a scan plan).
    pub fn new_read(&self) -> Result<TableRead<'a>> {
        let read_type = match &self.projected_fields {
            None => self.table.schema.fields().to_vec(),
            Some(projected) => self.resolve_projected_fields(projected)?,
        };

        Ok(TableRead::new(
            self.table,
            read_type,
            reader_pruning_predicates(self.filter.data_predicates.clone()),
        ))
    }

    fn resolve_projected_fields(&self, projected_fields: &[String]) -> Result<Vec<DataField>> {
        if projected_fields.is_empty() {
            return Ok(Vec::new());
        }

        let full_name = self.table.identifier().full_name();
        let field_map: HashMap<&str, &DataField> = self
            .table
            .schema
            .fields()
            .iter()
            .map(|field| (field.name(), field))
            .collect();

        let mut seen = HashSet::with_capacity(projected_fields.len());
        let mut resolved = Vec::with_capacity(projected_fields.len());

        for name in projected_fields {
            if !seen.insert(name.as_str()) {
                return Err(Error::ConfigInvalid {
                    message: format!("Duplicate projection column '{name}' for table {full_name}"),
                });
            }

            if name == crate::spec::ROW_ID_FIELD_NAME {
                resolved.push(DataField::new(
                    crate::spec::ROW_ID_FIELD_ID,
                    crate::spec::ROW_ID_FIELD_NAME.to_string(),
                    crate::spec::DataType::BigInt(crate::spec::BigIntType::with_nullable(true)),
                ));
                continue;
            }

            let field = field_map
                .get(name.as_str())
                .ok_or_else(|| Error::ColumnNotExist {
                    full_name: full_name.clone(),
                    column: name.clone(),
                })?;
            resolved.push((*field).clone());
        }

        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use crate::table::TableRead;
    mod test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../test_utils.rs"));
    }

    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BinaryRow, DataType, IntType, Predicate, PredicateBuilder, Schema, TableSchema, VarCharType,
    };
    use crate::table::{DataSplitBuilder, Table};
    use arrow_array::{Int32Array, RecordBatch};
    use futures::TryStreamExt;
    use std::fs;
    use tempfile::tempdir;
    use test_utils::{local_file_path, test_data_file, write_int_parquet_file};

    fn collect_int_column(batches: &[RecordBatch], column_name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                let column_index = batch.schema().index_of(column_name).unwrap();
                let array = batch.column(column_index);
                let values = array.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..values.len())
                    .map(|index| values.value(index))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn simple_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("dt", DataType::VarChar(VarCharType::string_type()))
                .column("id", DataType::Int(IntType::new()))
                .partition_keys(["dt"])
                .build()
                .unwrap(),
        );
        Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test-read-builder".to_string(),
            table_schema,
            None,
        )
    }

    #[test]
    fn test_exact_filter_pushdown_is_true_for_partition_only_filter() {
        let table = simple_table();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("dt", crate::spec::Datum::String("2024-01-01".to_string()))
            .unwrap();

        let builder = table.new_read_builder();

        assert!(builder.is_exact_filter_pushdown(&predicate));
    }

    #[test]
    fn test_exact_filter_pushdown_is_false_for_data_filter() {
        let table = simple_table();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_than("id", crate::spec::Datum::Int(1))
            .unwrap();

        let builder = table.new_read_builder();

        assert!(!builder.is_exact_filter_pushdown(&predicate));
    }

    #[tokio::test]
    async fn test_new_read_pushes_filter_to_reader_when_filter_column_not_projected() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(
            &parquet_path,
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![1, 2, 20, 30])],
            Some(2),
        );
        let file_size = fs::metadata(&parquet_path).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", crate::spec::Datum::Int(10))
            .unwrap();

        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![3, 4]);
    }

    #[tokio::test]
    async fn test_direct_table_read_with_filter_pushes_filter_to_reader() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(
            &parquet_path,
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![1, 2, 20, 30])],
            Some(2),
        );
        let file_size = fs::metadata(&parquet_path).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", crate::spec::Datum::Int(10))
            .unwrap();
        let read = TableRead::new(&table, vec![table.schema().fields()[0].clone()], Vec::new())
            .with_filter(predicate);
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![3, 4]);
    }

    #[tokio::test]
    async fn test_new_read_row_filter_filters_rows_within_matching_row_group() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(
            &parquet_path,
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![5, 20, 30, 40])],
            Some(2),
        );
        let file_size = fs::metadata(&parquet_path).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", crate::spec::Datum::Int(10))
            .unwrap();

        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_reader_pruning_ignores_partition_conjuncts() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("dt=2024-01-01").join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![1, 2, 20, 30])],
            Some(2),
        );
        let file_size = fs::metadata(bucket_dir.join("data.parquet")).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("dt", DataType::VarChar(VarCharType::string_type()))
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .partition_keys(["dt"])
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(1))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let predicate = Predicate::and(vec![
            PredicateBuilder::new(table.schema().fields())
                .equal("dt", crate::spec::Datum::String("2024-01-01".to_string()))
                .unwrap(),
            PredicateBuilder::new(table.schema().fields())
                .greater_or_equal("value", crate::spec::Datum::Int(10))
                .unwrap(),
        ]);

        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![3, 4]);
    }

    #[test]
    fn test_with_filter_extracts_row_id_ranges() {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test".to_string(),
            table_schema,
            None,
        );

        let mut builder = table.new_read_builder();
        let filter = Predicate::and(vec![
            Predicate::Leaf {
                column: crate::spec::ROW_ID_FIELD_NAME.to_string(),
                index: 0,
                data_type: DataType::BigInt(crate::spec::BigIntType::new()),
                op: crate::spec::PredicateOperator::GtEq,
                literals: vec![crate::spec::Datum::Long(10)],
            },
            Predicate::Leaf {
                column: crate::spec::ROW_ID_FIELD_NAME.to_string(),
                index: 0,
                data_type: DataType::BigInt(crate::spec::BigIntType::new()),
                op: crate::spec::PredicateOperator::LtEq,
                literals: vec![crate::spec::Datum::Long(20)],
            },
            PredicateBuilder::new(table.schema().fields())
                .equal("value", crate::spec::Datum::Int(42))
                .unwrap(),
        ]);
        builder.with_filter(filter);

        // _ROW_ID predicates should be extracted into row_ranges
        assert!(builder.row_ranges.is_some());
        let ranges = builder.row_ranges.as_ref().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from(), 10);
        assert_eq!(ranges[0].to(), 20);

        // _ROW_ID predicates should be removed from data_predicates
        assert!(!builder.filter.data_predicates.is_empty());
        for p in &builder.filter.data_predicates {
            if let Predicate::Leaf { column, .. } = p {
                assert_ne!(column, crate::spec::ROW_ID_FIELD_NAME);
            }
        }
    }

    #[test]
    fn test_with_filter_skips_extraction_when_row_ranges_set() {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test".to_string(),
            table_schema,
            None,
        );

        let mut builder = table.new_read_builder();
        builder.with_row_ranges(vec![crate::table::source::RowRange::new(0, 5)]);

        let filter = Predicate::Leaf {
            column: crate::spec::ROW_ID_FIELD_NAME.to_string(),
            index: 0,
            data_type: DataType::BigInt(crate::spec::BigIntType::new()),
            op: crate::spec::PredicateOperator::GtEq,
            literals: vec![crate::spec::Datum::Long(10)],
        };
        builder.with_filter(filter);

        // Explicit row_ranges should be preserved, not overwritten
        let ranges = builder.row_ranges.as_ref().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from(), 0);
        assert_eq!(ranges[0].to(), 5);
    }
}
