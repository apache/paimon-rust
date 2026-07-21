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
use super::format_read_builder::FormatReadBuilder;
use super::incremental_scan::{IncrementalScan, IncrementalScanMode};
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
pub struct ReadBuilder<'a>(ReadBuilderKind<'a>);

#[derive(Debug, Clone)]
enum ReadBuilderKind<'a> {
    Paimon(PaimonReadBuilder<'a>),
    Format(FormatReadBuilder<'a>),
}

impl<'a> ReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        if table.is_format_table() {
            Self(ReadBuilderKind::Format(FormatReadBuilder::new(table)))
        } else {
            Self(ReadBuilderKind::Paimon(PaimonReadBuilder::new(table)))
        }
    }

    /// Set column projection by name. Output order follows the caller-specified order.
    /// An empty list is a valid zero-column projection.
    ///
    /// Name resolution is deferred to scan/read build time (order-independent with
    /// [`with_case_sensitive`](Self::with_case_sensitive)). As a convenience, a
    /// column that cannot match under any case sensitivity (an obvious typo) is
    /// rejected here; case-dependent outcomes — a name that matches only
    /// case-insensitively, or a case-fold duplicate/ambiguity — surface from
    /// [`new_read`](Self::new_read) using the effective case sensitivity.
    pub fn with_projection(&mut self, columns: &[&str]) -> Result<&mut Self> {
        match &mut self.0 {
            ReadBuilderKind::Paimon(builder) => {
                builder.with_projection(columns)?;
            }
            ReadBuilderKind::Format(builder) => {
                builder.with_projection(columns)?;
            }
        }
        Ok(self)
    }

    /// Set whether column-name matching (projection and predicate column
    /// resolution) is case-sensitive. Defaults to `true` (exact match). When set
    /// to `false`, names are matched by ASCII case-folding and an ambiguous
    /// (case-colliding) request errors. This mirrors the per-read case
    /// sensitivity engines like Spark drive from `spark.sql.caseSensitive`,
    /// rather than being a table property.
    ///
    /// Projection resolution is lazy, so this affects a projection set via
    /// [`with_projection`](Self::with_projection) regardless of call order (the
    /// projected names are resolved at scan/read build time using the flag
    /// effective then). Predicates built via `PredicateBuilder` capture case
    /// sensitivity at their own construction time, so this flag does not
    /// retroactively change a predicate already passed to
    /// [`with_filter`](Self::with_filter).
    pub fn with_case_sensitive(&mut self, case_sensitive: bool) -> &mut Self {
        match &mut self.0 {
            ReadBuilderKind::Paimon(builder) => {
                builder.with_case_sensitive(case_sensitive);
            }
            ReadBuilderKind::Format(builder) => {
                builder.with_case_sensitive(case_sensitive);
            }
        }
        self
    }

    /// Set the full read type, including nested field pruning or connector-defined
    /// logical read types such as Variant extractions.
    pub fn with_read_type(&mut self, read_type: Vec<DataField>) -> &mut Self {
        match &mut self.0 {
            ReadBuilderKind::Paimon(builder) => {
                builder.with_read_type(read_type);
            }
            ReadBuilderKind::Format(builder) => {
                builder.with_read_type(read_type);
            }
        }
        self
    }

    /// Set a filter predicate for scan planning and conservative read pruning.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        match &mut self.0 {
            ReadBuilderKind::Paimon(builder) => {
                builder.with_filter(filter);
            }
            ReadBuilderKind::Format(builder) => {
                builder.with_filter(filter);
            }
        }
        self
    }

    /// Whether a translated predicate is exact at the table-provider boundary.
    pub fn is_exact_filter_pushdown(&self, filter: &Predicate) -> bool {
        match &self.0 {
            ReadBuilderKind::Paimon(builder) => builder.is_exact_filter_pushdown(filter),
            ReadBuilderKind::Format(builder) => builder.is_exact_filter_pushdown(filter),
        }
    }

    /// Set row ID ranges `[from, to]` (inclusive) for filtering in data evolution mode.
    pub fn with_row_ranges(&mut self, ranges: Vec<RowRange>) -> &mut Self {
        match &mut self.0 {
            ReadBuilderKind::Paimon(builder) => {
                builder.with_row_ranges(ranges);
            }
            ReadBuilderKind::Format(builder) => {
                builder.with_row_ranges(ranges);
            }
        }
        self
    }

    /// Push a row-limit hint down to scan planning.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        match &mut self.0 {
            ReadBuilderKind::Paimon(builder) => {
                builder.with_limit(limit);
            }
            ReadBuilderKind::Format(builder) => {
                builder.with_limit(limit);
            }
        }
        self
    }

    /// Create a table scan. Call [TableScan::plan] to get splits.
    pub fn new_scan(&self) -> TableScan<'a> {
        match &self.0 {
            ReadBuilderKind::Paimon(builder) => builder.new_scan(),
            ReadBuilderKind::Format(builder) => builder.new_scan(),
        }
    }

    /// Create a batch incremental scan over snapshot id range
    /// `(start_exclusive, end_inclusive]`.
    ///
    /// Filters and projection configured on this builder are pushed into the
    /// incremental plan (partition / bucket pruning on the delta path).
    pub fn new_incremental_scan(
        &self,
        mode: IncrementalScanMode,
        start_exclusive: i64,
        end_inclusive: i64,
    ) -> IncrementalScan<'a> {
        match &self.0 {
            ReadBuilderKind::Paimon(builder) => IncrementalScan::new(
                builder.table,
                builder.new_scan(),
                mode,
                start_exclusive,
                end_inclusive,
            ),
            // Format tables share the API surface; planning fails with Unsupported.
            ReadBuilderKind::Format(builder) => {
                IncrementalScan::for_table(builder.table(), mode, start_exclusive, end_inclusive)
            }
        }
    }

    /// Create a table read for consuming splits (e.g. from a scan plan).
    pub fn new_read(&self) -> Result<TableRead<'a>> {
        match &self.0 {
            ReadBuilderKind::Paimon(builder) => builder.new_read(),
            ReadBuilderKind::Format(builder) => builder.new_read(),
        }
    }
}

#[derive(Debug, Clone)]
struct PaimonReadBuilder<'a> {
    table: &'a Table,
    read_type: Option<Vec<DataField>>,
    /// Deferred projection column names. Resolved to `read_type` lazily at
    /// scan/read build time so `with_projection` and `with_case_sensitive`
    /// can be called in any order. Mutually exclusive with `read_type`
    /// ("last projection setter wins").
    projection_names: Option<Vec<String>>,
    filter: NormalizedFilter,
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
    case_sensitive: bool,
}

impl<'a> PaimonReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            read_type: None,
            projection_names: None,
            filter: NormalizedFilter::default(),
            limit: None,
            row_ranges: None,
            case_sensitive: true,
        }
    }

    /// Set column projection by name. Output order follows the caller-specified order.
    /// An empty list is a valid zero-column projection.
    ///
    /// Name resolution is deferred: the names are stored and resolved against the
    /// schema at scan/read build time using the case sensitivity effective then,
    /// so `with_projection` and `with_case_sensitive` are order-independent. As a
    /// convenience, columns that cannot match under any case sensitivity (a clear
    /// typo) are rejected here; case-dependent errors (case-only matches,
    /// duplicates, or ambiguity) surface from [`new_read`](Self::new_read).
    pub fn with_projection(&mut self, columns: &[&str]) -> Result<&mut Self> {
        let projection_names: Vec<String> = columns.iter().map(|c| (*c).to_string()).collect();
        validate_projection_possible(
            self.table.identifier().full_name(),
            self.table.schema.fields(),
            &projection_names,
        )?;
        self.projection_names = Some(projection_names);
        // A names projection supersedes any previously set read type.
        self.read_type = None;
        Ok(self)
    }

    /// Set whether column-name matching is case-sensitive. Defaults to `true`.
    pub fn with_case_sensitive(&mut self, case_sensitive: bool) -> &mut Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Set the full read type, including nested field pruning or connector-defined
    /// logical read types such as Variant extractions.
    pub fn with_read_type(&mut self, read_type: Vec<DataField>) -> &mut Self {
        self.read_type = Some(read_type);
        // An explicit read type supersedes any pending names projection.
        self.projection_names = None;
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
    ///
    /// Projection names are resolved here on a best-effort basis: the resolved
    /// read type only feeds the data-evolution `projected_read_field_ids`
    /// planning optimization, and a scan does not surface `Result`. If a pending
    /// names projection fails to resolve (unknown/ambiguous/duplicate column),
    /// the scan conservatively plans with no projection pushdown (correct, just
    /// less selective); the same projection resolution runs in `new_read`, which
    /// does return `Result` and surfaces the error to the caller. This keeps the
    /// scan infallible without silently discarding an error that a read would hit.
    pub fn new_scan(&self) -> TableScan<'a> {
        let partition_filter = self.filter.partition_predicate.clone().map(|pred| {
            PartitionFilter::from_predicate(pred, &self.table.schema().partition_fields())
        });
        let read_type = self.resolve_read_type().unwrap_or(None);
        TableScan::new(
            self.table,
            partition_filter,
            self.filter.data_predicates.clone(),
            self.filter.bucket_predicate.clone(),
            self.limit,
            self.row_ranges.clone(),
        )
        .with_projected_read_field_ids(projected_read_field_ids(&read_type))
    }

    /// Create a table read for consuming splits (e.g. from a scan plan).
    pub fn new_read(&self) -> Result<TableRead<'a>> {
        // Fail closed at read construction so bindings that short-circuit before
        // `to_arrow` (e.g. an empty-splits fast path) can't bypass the guard.
        let core_options = self.table.schema.core_options();
        core_options.ensure_read_authorized()?;
        let read_type = match self.resolve_read_type()? {
            None => self.table.schema.fields().to_vec(),
            Some(fields) => fields,
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

    /// Resolve the effective read type, deferring projection name resolution to
    /// the case sensitivity effective at build time (order-independent with
    /// `with_case_sensitive`).
    fn resolve_read_type(&self) -> Result<Option<Vec<DataField>>> {
        if let Some(read_type) = &self.read_type {
            return Ok(Some(read_type.clone()));
        }
        if let Some(names) = &self.projection_names {
            return Ok(Some(self.resolve_projected_fields(names)?));
        }
        Ok(None)
    }

    pub(super) fn resolve_projected_fields(
        &self,
        projection_names: &[String],
    ) -> Result<Vec<DataField>> {
        resolve_projected_fields(
            self.table.identifier().full_name(),
            self.table.schema.fields(),
            projection_names,
            self.case_sensitive,
        )
    }
}

/// Best-effort early validation for `with_projection`: reject only columns that
/// cannot match the schema under *any* case sensitivity, so an obvious typo
/// (e.g. `foo`) fails fast at call time. Case-dependent outcomes (a name that
/// matches only case-insensitively, or a case-fold ambiguity) are left to the
/// final resolution in `new_scan`/`new_read`, which uses the effective
/// `case_sensitive` — keeping `with_projection` and `with_case_sensitive`
/// order-independent.
pub(super) fn validate_projection_possible(
    full_name: String,
    fields: &[DataField],
    projection_names: &[String],
) -> Result<()> {
    // Fold the schema names once (O(fields)) so validation is O(fields +
    // projections) rather than scanning the whole schema per projected name.
    let folded_names: HashSet<String> = fields
        .iter()
        .map(|f| f.name().to_ascii_lowercase())
        .collect();
    for name in projection_names {
        if name == crate::spec::ROW_ID_FIELD_NAME {
            continue;
        }
        if !folded_names.contains(&name.to_ascii_lowercase()) {
            return Err(Error::ColumnNotExist {
                full_name: full_name.clone(),
                column: name.clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn resolve_projected_fields(
    full_name: String,
    fields: &[DataField],
    projection_names: &[String],
    case_sensitive: bool,
) -> Result<Vec<DataField>> {
    if projection_names.is_empty() {
        return Ok(Vec::new());
    }

    // Build the name index once (O(fields)) so resolution is O(fields +
    // projections) rather than scanning the whole schema per projected name.
    // Case-sensitive: exact name -> field. Case-insensitive: ASCII-folded name
    // -> the unique field, or `None` when two or more fields collide under
    // folding (ambiguous, mirroring Spark's `AMBIGUOUS` behavior).
    let sensitive_index: HashMap<&str, &DataField> = if case_sensitive {
        fields.iter().map(|f| (f.name(), f)).collect()
    } else {
        HashMap::new()
    };
    let mut folded_index: HashMap<String, Option<&DataField>> = HashMap::new();
    if !case_sensitive {
        for f in fields {
            folded_index
                .entry(f.name().to_ascii_lowercase())
                .and_modify(|slot| *slot = None)
                .or_insert(Some(f));
        }
    }

    let mut seen: HashSet<String> = HashSet::with_capacity(projection_names.len());
    let mut resolved = Vec::with_capacity(projection_names.len());

    for name in projection_names {
        // Dedup under the same case sensitivity used for resolution: with
        // `case-sensitive=false`, `["Name","name"]` must flag a duplicate rather
        // than resolve the same field twice.
        let dedup_key = if case_sensitive {
            name.clone()
        } else {
            name.to_ascii_lowercase()
        };
        if !seen.insert(dedup_key) {
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

        let field = if case_sensitive {
            sensitive_index.get(name.as_str()).copied()
        } else {
            match folded_index.get(&name.to_ascii_lowercase()) {
                Some(Some(f)) => Some(*f),
                Some(None) => {
                    return Err(Error::ConfigInvalid {
                        message: format!(
                            "Ambiguous projection column '{name}' for table {full_name}: multiple fields match case-insensitively"
                        ),
                    });
                }
                None => None,
            }
        };
        let field = field.ok_or_else(|| Error::ColumnNotExist {
            full_name: full_name.clone(),
            column: name.clone(),
        })?;
        resolved.push(field.clone());
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
    use super::{PaimonReadBuilder, ReadBuilder, ReadBuilderKind};
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;
    use test_utils::{local_file_path, test_data_file, write_int_parquet_file};

    fn paimon_builder<'a, 'b>(builder: &'b ReadBuilder<'a>) -> &'b PaimonReadBuilder<'a> {
        match &builder.0 {
            ReadBuilderKind::Paimon(inner) => inner,
            ReadBuilderKind::Format(_) => panic!("expected Paimon read builder"),
        }
    }

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
        // A column that cannot match under any case sensitivity is an obvious
        // typo and is rejected early by with_projection (possible-validation).
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
        // Resolution is deferred: the duplicate error surfaces at new_read().
        let table = simple_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["id", "id"]).unwrap();
        let err = builder.new_read().unwrap_err();

        assert!(matches!(
            err,
            crate::Error::ConfigInvalid { message }
                if message.contains("Duplicate projection column 'id'")
        ));
    }

    fn mixed_case_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("Name", DataType::VarChar(VarCharType::new(50).unwrap()))
                .build()
                .unwrap(),
        );
        Table::new(
            file_io,
            Identifier::new("default", "t"),
            "/tmp/test-read-builder-ci".to_string(),
            table_schema,
            None,
        )
    }

    #[test]
    fn test_read_builder_default_case_sensitive_rejects_wrong_case() {
        // Default (case-sensitive=true): a wrong-case projection must not resolve.
        // Resolution is deferred, so the error surfaces at new_read().
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["NAME"]).unwrap();
        let err = builder.new_read().unwrap_err();
        assert!(matches!(err, crate::Error::ColumnNotExist { .. }));
    }

    #[test]
    fn test_read_builder_with_case_sensitive_false_resolves_to_canonical() {
        // After with_case_sensitive(false), a wrong-case projection resolves to
        // the canonical schema field name.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder
            .with_case_sensitive(false)
            .with_projection(&["nAmE"])
            .unwrap();
        let read_type = paimon_builder(&builder)
            .resolve_read_type()
            .unwrap()
            .unwrap();
        assert_eq!(read_type.len(), 1);
        assert_eq!(read_type[0].name(), "Name");
    }

    #[test]
    fn test_projection_then_case_sensitive_false_is_order_independent() {
        // with_projection BEFORE with_case_sensitive(false): the wrong-case name
        // still resolves case-insensitively because resolution is deferred.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["name"]).unwrap();
        builder.with_case_sensitive(false);
        let read = builder.new_read().unwrap();
        assert_eq!(read.read_type().len(), 1);
        assert_eq!(read.read_type()[0].name(), "Name");
    }

    #[test]
    fn test_case_sensitive_false_then_projection_is_order_independent() {
        // with_case_sensitive(false) BEFORE with_projection: same result.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_case_sensitive(false);
        builder.with_projection(&["name"]).unwrap();
        let read = builder.new_read().unwrap();
        assert_eq!(read.read_type().len(), 1);
        assert_eq!(read.read_type()[0].name(), "Name");
    }

    #[test]
    fn test_default_case_sensitive_wrong_case_errors_at_new_read() {
        // Default (no with_case_sensitive) + wrong-case projection errors at read.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["name"]).unwrap();
        let err = builder.new_read().unwrap_err();
        assert!(matches!(err, crate::Error::ColumnNotExist { .. }));
    }

    #[test]
    fn test_new_scan_defers_projection_error_to_new_read() {
        // Contract: new_scan is infallible — an unresolved projection degrades to
        // no projection pushdown (correct, less selective) rather than erroring,
        // while the same resolution surfaces the error from new_read.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["name"]).unwrap(); // case-only match, default sensitive
        let _scan = builder.new_scan(); // must not panic / must succeed
        let err = builder.new_read().unwrap_err();
        assert!(matches!(err, crate::Error::ColumnNotExist { .. }));
    }

    #[test]
    fn test_case_only_match_passes_early_validation() {
        // Hybrid: a name matching only case-insensitively (`name` vs schema
        // `Name`) is NOT rejected by with_projection — its outcome depends on the
        // final case sensitivity, so it is deferred. Only names that cannot match
        // under any case sensitivity fail early (covered by
        // test_with_projection_validates_unknown_projection).
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        assert!(builder.with_projection(&["name"]).is_ok());
    }

    #[test]
    fn test_read_type_after_projection_wins() {
        // with_read_type after with_projection: the explicit read type wins and
        // the pending names projection is cleared.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_projection(&["id"]).unwrap();
        builder.with_read_type(vec![DataField::new(
            1,
            "Name".to_string(),
            DataType::VarChar(VarCharType::new(50).unwrap()),
        )]);
        let read_type = paimon_builder(&builder)
            .resolve_read_type()
            .unwrap()
            .unwrap();
        assert_eq!(read_type.len(), 1);
        assert_eq!(read_type[0].name(), "Name");
    }

    #[test]
    fn test_projection_after_read_type_wins() {
        // with_projection after with_read_type: the names projection wins and the
        // explicit read type is cleared.
        let table = mixed_case_table();
        let mut builder = ReadBuilder::new(&table);
        builder.with_read_type(vec![DataField::new(
            1,
            "Name".to_string(),
            DataType::VarChar(VarCharType::new(50).unwrap()),
        )]);
        builder.with_projection(&["id"]).unwrap();
        let read_type = paimon_builder(&builder)
            .resolve_read_type()
            .unwrap()
            .unwrap();
        assert_eq!(read_type.len(), 1);
        assert_eq!(read_type[0].name(), "id");
    }

    fn ci_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "Name".to_string(),
                DataType::VarChar(VarCharType::new(50).unwrap()),
            ),
        ]
    }

    #[test]
    fn test_resolve_projection_case_sensitive_exact() {
        // Default (case-sensitive): exact names resolve, wrong case does not.
        let out = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["Name".into()],
            true,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name(), "Name");

        let err = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["NAME".into()],
            true,
        )
        .unwrap_err();
        assert!(matches!(err, crate::Error::ColumnNotExist { .. }));
    }

    #[test]
    fn test_resolve_projection_case_insensitive_matches_and_keeps_canonical() {
        // Case-insensitive: wrong-case request resolves to the canonical field.
        let out = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["nAmE".into()],
            false,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name(), "Name", "canonical schema name is preserved");
        assert_eq!(out[0].id(), 1);
    }

    #[test]
    fn test_resolve_projection_case_insensitive_ambiguous_errors() {
        let fields = vec![
            DataField::new(0, "Col".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "col".to_string(), DataType::Int(IntType::new())),
        ];
        let err =
            super::resolve_projected_fields("db.t".to_string(), &fields, &["COL".into()], false)
                .unwrap_err();
        assert!(matches!(err, crate::Error::ConfigInvalid { .. }));
    }

    #[test]
    fn test_resolve_projection_case_insensitive_dedups_by_folded_name() {
        // With case-insensitive matching, `["Name","name"]` both resolve to the
        // canonical `Name` field, so it must be flagged as a duplicate rather
        // than returning the column twice.
        let err = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["Name".into(), "name".into()],
            false,
        )
        .unwrap_err();
        assert!(matches!(err, crate::Error::ConfigInvalid { message }
            if message.contains("Duplicate projection column")));

        // A single request still resolves cleanly.
        let out = super::resolve_projected_fields(
            "db.t".to_string(),
            &ci_fields(),
            &["Name".into()],
            false,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name(), "Name");
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
    async fn test_direct_raw_read_falls_back_when_external_row_filter_fails() {
        #[derive(Debug)]
        struct CountingFactory(Arc<AtomicUsize>);

        impl crate::arrow::RowFilterFactory for CountingFactory {
            fn create(
                &self,
                _context: crate::arrow::RowFilterContext<'_>,
            ) -> crate::Result<Vec<Box<dyn crate::arrow::RowFilter>>> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Err(crate::Error::UnexpectedError {
                    message: "test decoder-filter failure".to_string(),
                    source: None,
                })
            }
        }

        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();
        let parquet_path = bucket_dir.join("data.parquet");
        write_int_parquet_file(&parquet_path, vec![("id", vec![1, 2])], None);
        let file_size = fs::metadata(&parquet_path).unwrap().len() as i64;

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
            .with_data_files(vec![test_data_file("data.parquet", 2, file_size)])
            .build()
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));

        let batches = TableRead::new(&table, table.schema().fields().to_vec(), Vec::new())
            .with_row_filter_factory(Arc::new(CountingFactory(Arc::clone(&calls))))
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(collect_int_column(&batches, "id"), vec![1, 2]);
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
        let inner = paimon_builder(&builder);
        assert!(inner.row_ranges.is_some());
        let ranges = inner.row_ranges.as_ref().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from(), 10);
        assert_eq!(ranges[0].to(), 20);

        // _ROW_ID predicates should be removed from data_predicates
        assert!(!inner.filter.data_predicates.is_empty());
        for p in &inner.filter.data_predicates {
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
        let ranges = paimon_builder(&builder).row_ranges.as_ref().unwrap();
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
