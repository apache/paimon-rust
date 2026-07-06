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
use super::partition_filter::PartitionFilter;
use super::table_read::TableRead;
use super::{Table, TableScan};
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
    read_type: Option<Vec<DataField>>,
    filter: NormalizedFilter,
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
}

impl<'a> ReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            read_type: None,
            filter: NormalizedFilter::default(),
            limit: None,
            row_ranges: None,
        }
    }

    /// Set column projection by name. Output order follows the caller-specified order.
    /// Unknown or duplicate names cause this method to fail; an empty list is a valid
    /// zero-column projection.
    pub fn with_projection(&mut self, columns: &[&str]) -> Result<&mut Self> {
        let projection_names = columns.iter().map(|c| (*c).to_string()).collect::<Vec<_>>();
        self.read_type = Some(self.resolve_projected_fields(&projection_names)?);
        Ok(self)
    }

    /// Set the full read type, including nested field pruning or connector-defined
    /// logical read types such as Variant extractions.
    pub fn with_read_type(&mut self, read_type: Vec<DataField>) -> &mut Self {
        self.read_type = Some(read_type);
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
    /// [`TableRead`] may use supported non-partition data predicates on formats
    /// with reader pruning for conservative row-group pruning. Parquet may also
    /// use native row filtering. Row-level exactness is enforced on all read
    /// paths: format readers apply an exact residual filter on append reads
    /// (see `FormatFileReader::read_batch_stream` for per-format exceptions),
    /// data-evolution reads filter batches exactly before yielding, and
    /// primary-key merge reads push key conjuncts below the merge and enforce
    /// the full predicate with an exact post-merge residual filter.
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
        let partition_filter = self.filter.partition_predicate.clone().map(|pred| {
            PartitionFilter::from_predicate(pred, &self.table.schema().partition_fields())
        });
        TableScan::new(
            self.table,
            partition_filter,
            self.filter.data_predicates.clone(),
            self.filter.bucket_predicate.clone(),
            self.limit,
            self.row_ranges.clone(),
        )
        .with_projected_read_field_ids(projected_read_field_ids(&self.read_type))
    }

    /// Create a table read for consuming splits (e.g. from a scan plan).
    pub fn new_read(&self) -> Result<TableRead<'a>> {
        // Fail closed at read construction so bindings that short-circuit before
        // `to_arrow` (e.g. an empty-splits fast path) can't bypass the guard.
        CoreOptions::new(self.table.schema.options()).ensure_read_authorized()?;
        let read_type = match &self.read_type {
            None => self.table.schema.fields().to_vec(),
            Some(fields) => fields.clone(),
        };

        // Pass the FULL data predicate through (including `And`/`Or`/`Not`).
        // Pushdown/stats skip compound nodes; the residual pass enforces the full
        // predicate exactly. Pruning here would drop compound predicates.
        Ok(TableRead::new(
            self.table,
            read_type,
            self.filter.data_predicates.clone(),
        ))
    }

    pub(super) fn resolve_projected_fields(
        &self,
        projection_names: &[String],
    ) -> Result<Vec<DataField>> {
        resolve_projected_fields(
            self.table.identifier().full_name(),
            self.table.schema.fields(),
            projection_names,
        )
    }
}

pub(super) fn resolve_projected_fields(
    full_name: String,
    fields: &[DataField],
    projection_names: &[String],
) -> Result<Vec<DataField>> {
    if projection_names.is_empty() {
        return Ok(Vec::new());
    }

    let field_map: HashMap<&str, &DataField> =
        fields.iter().map(|field| (field.name(), field)).collect();

    let mut seen = HashSet::with_capacity(projection_names.len());
    let mut resolved = Vec::with_capacity(projection_names.len());

    for name in projection_names {
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

pub(super) fn projected_read_field_ids_from_fields(fields: &[DataField]) -> HashSet<i32> {
    fields
        .iter()
        .filter(|field| !is_system_projection_field(field.id()))
        .map(|field| field.id())
        .collect::<HashSet<_>>()
}

fn projected_read_field_ids(read_type: &Option<Vec<DataField>>) -> Option<HashSet<i32>> {
    read_type
        .as_ref()
        .map(|fields| projected_read_field_ids_from_fields(fields))
}

pub(super) fn is_system_projection_field(field_id: i32) -> bool {
    matches!(
        field_id,
        crate::spec::ROW_ID_FIELD_ID
            | crate::spec::SEQUENCE_NUMBER_FIELD_ID
            | crate::spec::VALUE_KIND_FIELD_ID
    )
}

#[cfg(test)]
mod tests {
    use super::ReadBuilder;
    use crate::table::TableRead;
    mod test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../test_utils.rs"));
    }

    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BinaryRow, DataField, DataType, IntType, Predicate, PredicateBuilder, Schema, TableSchema,
        VarCharType,
    };
    use crate::table::{query_auth_table, DataSplitBuilder, Table};
    use arrow_array::{Int32Array, RecordBatch};
    use futures::TryStreamExt;
    use std::collections::{HashMap, HashSet};
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

    fn partial_update_dv_pk_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "partial-update")
                .option("deletion-vectors.enabled", "true")
                .build()
                .unwrap(),
        );
        Table::new(
            file_io,
            Identifier::new("default", "partial_update_dv_t"),
            "/tmp/test-partial-update-dv-read-builder".to_string(),
            table_schema,
            None,
        )
    }

    #[test]
    fn test_read_fails_closed_when_query_auth_enabled() {
        let table = query_auth_table();
        // `new_read` fails closed, so bindings that short-circuit before `to_arrow` can't bypass.
        let err = table.new_read_builder().new_read().unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "building a read for a query-auth.enabled table must fail closed"
        );
    }

    #[test]
    fn test_dynamic_option_cannot_disable_query_auth() {
        // Copying the table with the option off must not weaken a stored `true`.
        let table = query_auth_table().copy_with_options(HashMap::from([(
            "query-auth.enabled".to_string(),
            "false".to_string(),
        )]));
        let err = table.new_read_builder().new_read().unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "a dynamic override must not disable query-auth"
        );
    }

    #[test]
    fn test_projected_read_field_ids_uses_projection_ids() {
        let read_type = vec![DataField::new(
            1,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )];

        assert_eq!(
            super::projected_read_field_ids_from_fields(&read_type),
            HashSet::from([1])
        );
    }

    #[test]
    fn test_projected_read_field_ids_ignores_system_only_projection() {
        let read_type = vec![DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            crate::spec::ROW_ID_FIELD_NAME.to_string(),
            DataType::Int(IntType::new()),
        )];

        assert_eq!(
            super::projected_read_field_ids_from_fields(&read_type),
            HashSet::new()
        );
    }

    #[test]
    fn test_with_projection_validates_unknown_projection() {
        let table = simple_table();
        let mut builder = ReadBuilder::new(&table);
        let err = builder.with_projection(&["missing"]).unwrap_err();

        assert!(matches!(
            err,
            crate::Error::ColumnNotExist {
                full_name,
                column,
            } if full_name == "default.t" && column == "missing"
        ));
    }

    #[test]
    fn test_with_projection_validates_duplicate_projection() {
        let table = simple_table();
        let mut builder = ReadBuilder::new(&table);
        let err = builder.with_projection(&["id", "id"]).unwrap_err();

        assert!(matches!(
            err,
            crate::Error::ConfigInvalid { message }
                if message.contains("Duplicate projection column 'id'")
        ));
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
        builder
            .with_projection(&["id"])
            .unwrap()
            .with_filter(predicate);
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
        builder
            .with_projection(&["id"])
            .unwrap()
            .with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![2, 3, 4]);
    }

    /// Real-path regression: an `Or` predicate must be applied exactly through the
    /// public ReadBuilder/TableRead path. The data predicate set is no longer
    /// pruned before the reader, so the residual pass receives the full `Or` and
    /// filters exactly. Single row group so stats pruning cannot exclude anything.
    #[tokio::test]
    async fn test_new_read_applies_or_predicate_exactly_via_public_path() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(
            &parquet_path,
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![5, 20, 30, 40])],
            None,
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

        // id = 1 OR value = 40  ->  rows {id=1} and {id=4}.
        let pb = PredicateBuilder::new(table.schema().fields());
        let predicate = crate::spec::Predicate::or(vec![
            pb.equal("id", crate::spec::Datum::Int(1)).unwrap(),
            pb.equal("value", crate::spec::Datum::Int(40)).unwrap(),
        ]);

        let mut builder = table.new_read_builder();
        builder
            .with_projection(&["id"])
            .unwrap()
            .with_filter(predicate);
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_int_column(&batches, "id"), vec![1, 4]);
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
        builder
            .with_projection(&["id"])
            .unwrap()
            .with_filter(predicate);
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

    #[tokio::test]
    async fn test_direct_table_read_rejects_partial_update_with_deletion_vectors() {
        let table = partial_update_dv_pk_table();
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("/tmp/test-partial-update-dv-read-builder/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 1, 0)])
            .with_data_deletion_files(vec![Some(crate::table::source::DeletionFile::new(
                "/tmp/test-partial-update-dv-read-builder/index/dv".to_string(),
                0,
                0,
                None,
            ))])
            .build()
            .unwrap();
        let err = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new())
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("deletion vectors")),
            "expected partial-update+DV read to fail fast with Unsupported, got {err:?}"
        );
    }
}
