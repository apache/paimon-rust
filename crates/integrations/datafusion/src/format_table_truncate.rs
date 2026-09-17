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

//! TRUNCATE TABLE for Format Tables.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::Expr as SqlExpr;
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{Partition, PartitionStatistics};
use paimon::table::{FormatTableTruncator, Table};

use crate::error::to_datafusion_error;
use crate::format_partition_ddl::{has_custom_location, leading_partition_prefix};
use crate::sql_context::{ok_result, SQLContext};

/// `TRUNCATE TABLE t [PARTITION (...)]` on a Format Table. Deletes data files only: directories and
/// registrations stay. Mirrors Java `FormatTableCommit#truncateTable` and `#truncatePartitions`.
pub(crate) async fn execute_truncate(
    ctx: &SQLContext,
    catalog: &dyn Catalog,
    identifier: &Identifier,
    table: &Table,
    partitions: Option<&[SqlExpr]>,
    enable_ident_normalization: bool,
) -> DFResult<DataFrame> {
    let truncator = FormatTableTruncator::new(table);
    if table.schema().partition_keys().is_empty() {
        if partitions.is_some() {
            return Err(DataFusionError::Plan(format!(
                "TRUNCATE TABLE ... PARTITION requires a partitioned table, but {} is not \
                 partitioned",
                identifier.full_name()
            )));
        }
        truncator
            .truncate_unpartitioned()
            .await
            .map_err(to_datafusion_error)?;
        return ok_result(ctx.ctx());
    }
    let prefix = leading_partition_prefix(
        partitions.unwrap_or_default(),
        table,
        "TRUNCATE TABLE",
        enable_ident_normalization,
    )?;
    // Unlike ANALYZE, a bare `PARTITION (dt)` is not read as every value: that empties the table.
    if partitions.is_some() && prefix.is_empty() {
        return Err(DataFusionError::Plan(
            "PARTITION clause requires at least one column = value".to_string(),
        ));
    }

    // A table with catalog-managed partitions has the partitions its catalog holds; any other
    // table has the partition directories below it, as its scan reads them.
    let catalog_managed = table.has_catalog_managed_partitions();
    let selected = if catalog_managed {
        registered_partitions(catalog, identifier, table, &prefix).await?
    } else {
        truncator
            .discover_partitions()
            .await
            .map_err(to_datafusion_error)?
            .into_iter()
            .filter(|spec| matches_prefix(spec, &prefix))
            .collect()
    };
    if selected.is_empty() && !prefix.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Partition {prefix:?} does not exist in table {}",
            identifier.full_name()
        )));
    }

    let truncated_at = chrono::Utc::now().timestamp_millis();
    let mut emptied = Vec::with_capacity(selected.len());
    let mut failure = None;
    for spec in selected {
        match truncator.truncate_partition(&spec).await {
            Ok(()) => emptied.push(spec),
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    // What was emptied is reported even when a later partition failed, so the catalog stops
    // describing files that are gone; the deletion failure is the one returned.
    if catalog_managed && !emptied.is_empty() {
        let statistics = emptied
            .iter()
            .map(|spec| empty_statistics(spec, truncated_at))
            .collect();
        let reported = catalog
            .create_partitions_with_statistics(identifier, emptied, true, Some(statistics), true)
            .await;
        if let (Err(error), None) = (reported, &failure) {
            return Err(to_datafusion_error(error));
        }
    }
    if let Some(error) = failure {
        return Err(to_datafusion_error(error));
    }
    ok_result(ctx.ctx())
}

/// The registered partitions under `prefix`. Nothing is deleted when one of them sits at a custom
/// location, which this client cannot reset to its default directory as Java does.
async fn registered_partitions(
    catalog: &dyn Catalog,
    identifier: &Identifier,
    table: &Table,
    prefix: &[(String, String)],
) -> DFResult<Vec<HashMap<String, String>>> {
    let complete = prefix.len() == table.schema().partition_keys().len();
    let partitions = if complete {
        let spec = prefix.iter().cloned().collect::<HashMap<_, _>>();
        catalog
            .list_partitions_by_names(identifier, vec![spec])
            .await
    } else {
        catalog.list_partitions(identifier).await
    }
    .map_err(to_datafusion_error)?
    .into_iter()
    .filter(|partition| matches_prefix(&partition.spec, prefix))
    .collect::<Vec<_>>();

    let custom_located = partitions
        .iter()
        .filter(|partition| has_custom_location(partition))
        .map(|partition| &partition.spec)
        .collect::<Vec<_>>();
    if !custom_located.is_empty() {
        return Err(DataFusionError::NotImplemented(format!(
            "TRUNCATE TABLE cannot empty partitions with a custom location in Format Table {}: \
             {custom_located:?}",
            identifier.full_name()
        )));
    }
    Ok(partitions
        .into_iter()
        .map(|partition| partition.spec)
        .collect())
}

fn matches_prefix(spec: &HashMap<String, String>, prefix: &[(String, String)]) -> bool {
    prefix
        .iter()
        .all(|(key, value)| spec.get(key) == Some(value))
}

/// What an emptied partition holds, dated to the truncation.
fn empty_statistics(spec: &HashMap<String, String>, truncated_at: i64) -> PartitionStatistics {
    PartitionStatistics {
        spec: spec.clone(),
        record_count: 0,
        file_size_in_bytes: 0,
        file_count: 0,
        last_file_creation_time: truncated_at,
        total_buckets: Partition::UNKNOWN_TOTAL_BUCKETS,
    }
}
