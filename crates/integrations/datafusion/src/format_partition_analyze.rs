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

//! ANALYZE TABLE for Format Tables with catalog-managed partitions.

use std::collections::HashSet;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{Analyze, Expr as SqlExpr};
use paimon::table::FormatTablePartitionStatsCollector;

use crate::error::to_datafusion_error;
use crate::format_partition_ddl::{
    ensure_catalog_managed_format_table, has_custom_location, parse_format_partition_spec,
};
use crate::sql_context::{
    normalize_schema_identifier, ok_result, partition_assignment, SQLContext,
};

/// `ANALYZE TABLE t [PARTITION (...)] COMPUTE STATISTICS [NOSCAN]` on a Format Table with
/// catalog-managed partitions.
///
/// The registered partitions are measured from storage and each measured field replaces what
/// the catalog holds, which is how a table catches up with writers the catalog never saw.
/// NOSCAN stops at what a listing gives, file count, byte size and last file creation time,
/// while a full ANALYZE also reads every file footer for its row count.
///
/// Analyzing never adds or removes a partition: it measures the ones registered when it
/// listed them. There is no lock between that listing and the write, so a partition dropped
/// in between can come back with its last measurement, the same last-writer-wins window every
/// lock-free partition operation on these tables has.
///
/// Mirrors Java `PaimonAnalyzeFormatTablePartitionsCommand`.
pub(crate) async fn execute_analyze(
    ctx: &SQLContext,
    analyze: &Analyze,
    enable_ident_normalization: bool,
) -> DFResult<DataFrame> {
    let Some(table_name) = &analyze.table_name else {
        return Err(DataFusionError::Plan(
            "ANALYZE requires a table name".to_string(),
        ));
    };
    if analyze.for_columns || !analyze.columns.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "ANALYZE TABLE ... FOR COLUMNS is not supported: a Format Table has nowhere to \
             keep column statistics"
                .to_string(),
        ));
    }
    if analyze.cache_metadata {
        return Err(DataFusionError::NotImplemented(
            "ANALYZE TABLE ... CACHE METADATA is not supported".to_string(),
        ));
    }
    if !analyze.compute_statistics {
        return Err(DataFusionError::Plan(
            "ANALYZE TABLE requires COMPUTE STATISTICS".to_string(),
        ));
    }
    SQLContext::ensure_partition_command_target(table_name, "ANALYZE TABLE")?;
    let (catalog, _catalog_name, identifier) = ctx.resolve_catalog_and_table(table_name)?;
    let table = catalog
        .get_table(&identifier)
        .await
        .map_err(to_datafusion_error)?;
    ensure_catalog_managed_format_table(&table, "ANALYZE TABLE")?;
    let prefix = analyze_partition_prefix(
        analyze.partitions.as_deref().unwrap_or_default(),
        &table,
        enable_ident_normalization,
    )?;

    let selected = catalog
        .list_partitions(&identifier)
        .await
        .map_err(to_datafusion_error)?
        .into_iter()
        .filter(|partition| {
            prefix
                .iter()
                .all(|(key, value)| partition.spec.get(key) == Some(value))
        })
        .collect::<Vec<_>>();
    if selected.is_empty() && !prefix.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Partition {prefix:?} does not exist in table {}",
            identifier.full_name()
        )));
    }
    let custom_located = selected
        .iter()
        .filter(|partition| has_custom_location(partition))
        .map(|partition| &partition.spec)
        .collect::<Vec<_>>();
    if !custom_located.is_empty() {
        return Err(DataFusionError::NotImplemented(format!(
            "ANALYZE TABLE cannot measure partitions with a custom location in Format Table \
             {}: {custom_located:?}",
            identifier.full_name()
        )));
    }
    if selected.is_empty() {
        return ok_result(ctx.ctx());
    }

    let specs = selected
        .into_iter()
        .map(|partition| partition.spec)
        .collect::<Vec<_>>();
    let statistics = FormatTablePartitionStatsCollector::new(
        &table,
        !analyze.noscan,
        format_table_statistics_parallelism(ctx),
    )
    .collect(&specs)
    .await
    .map_err(to_datafusion_error)?;
    catalog
        .create_partitions_with_statistics(&identifier, specs, true, Some(statistics), true)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx.ctx())
}

/// `format-table.statistics.parallelism` from the session (`SET 'paimon.<key>'`), default 8.
/// A value below one is read as one.
///
/// Like Java's Spark connector option of the same name, it is a session setting, not a table
/// option.
fn format_table_statistics_parallelism(ctx: &SQLContext) -> usize {
    const KEY: &str = "format-table.statistics.parallelism";
    ctx.dynamic_options()
        .read()
        .unwrap()
        .get(KEY)
        .and_then(|value| value.trim().parse::<i64>().ok())
        .map(|value| value.max(1) as usize)
        .unwrap_or(8)
}

/// The values an `ANALYZE ... PARTITION (...)` clause fixes, in partition-key order.
///
/// A column named without a value means every value of it, and the columns that carry a value
/// must be a leading run of the partition keys: `PARTITION (dt = 'x', hour)` selects every hour of
/// that day, while `PARTITION (hour = '00')` is rejected rather than quietly widened to more
/// partitions than were asked for. Values are spelled the way ADD PARTITION writes them, so
/// `p = '01'` selects the INT partition registered as `1`.
fn analyze_partition_prefix(
    expressions: &[SqlExpr],
    table: &paimon::Table,
    enable_ident_normalization: bool,
) -> DFResult<Vec<(String, String)>> {
    let partition_keys = table.schema().partition_keys();
    let mut named = HashSet::with_capacity(expressions.len());
    let mut assignments = Vec::with_capacity(expressions.len());
    for expression in expressions {
        let column = match expression {
            SqlExpr::Identifier(identifier) => {
                normalize_schema_identifier(identifier, enable_ident_normalization)
            }
            other => {
                let (column, _) = partition_assignment(other, enable_ident_normalization)?;
                assignments.push(other.clone());
                column
            }
        };
        if !partition_keys.contains(&column) {
            return Err(DataFusionError::Plan(format!(
                "Column '{column}' is not a partition column"
            )));
        }
        if !named.insert(column.clone()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate partition column '{column}'"
            )));
        }
    }
    let spec = parse_format_partition_spec(
        &assignments,
        table,
        false,
        Some("ANALYZE TABLE"),
        enable_ident_normalization,
    )?;
    let leading = partition_keys
        .iter()
        .take_while(|key| spec.contains_key(key.as_str()))
        .count();
    if leading != spec.len() {
        return Err(DataFusionError::Plan(format!(
            "ANALYZE TABLE {} PARTITION must give values for a leading run of its partition \
             columns {partition_keys:?}",
            table.identifier().full_name()
        )));
    }
    Ok(partition_keys[..leading]
        .iter()
        .map(|key| (key.clone(), spec[key].clone()))
        .collect())
}
