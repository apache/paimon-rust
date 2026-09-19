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

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::Analyze;
use paimon::table::FormatTablePartitionStatsCollector;

use crate::error::to_datafusion_error;
use crate::format_partition_ddl::{
    ensure_catalog_managed_format_table, has_custom_location, leading_partition_prefix,
};
use crate::sql_context::{ok_result, SQLContext};

/// `ANALYZE TABLE t [PARTITION (...)] COMPUTE STATISTICS [NOSCAN]` on a Format Table with
/// catalog-managed partitions. Mirrors Java `PaimonAnalyzeFormatTablePartitionsCommand`.
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
    let prefix = leading_partition_prefix(
        analyze.partitions.as_deref().unwrap_or_default(),
        &table,
        "ANALYZE TABLE",
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
