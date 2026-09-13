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

//! MSCK REPAIR TABLE for Format Tables with catalog-managed partitions.

use std::collections::{BTreeMap, HashMap, HashSet};

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{AddDropSync, Msck};
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::CoreOptions;
use paimon::table::{FormatTablePartitionPaths, Table};

use crate::error::to_datafusion_error;
use crate::format_partition_ddl::ensure_catalog_managed_format_table;
use crate::sql_context::{ok_result, SQLContext};

pub(crate) async fn execute_msck(ctx: &SQLContext, msck: &Msck) -> DFResult<DataFrame> {
    if !msck.repair {
        return Err(DataFusionError::Plan(
            "MSCK requires the REPAIR keyword".to_string(),
        ));
    }
    SQLContext::ensure_partition_command_target(&msck.table_name, "MSCK REPAIR TABLE")?;
    let (catalog, _catalog_name, identifier) = ctx.resolve_catalog_and_table(&msck.table_name)?;
    let table = catalog
        .get_table(&identifier)
        .await
        .map_err(to_datafusion_error)?;
    ensure_catalog_managed_format_table(&table, "MSCK REPAIR TABLE")?;
    let mode = match msck.partition_action {
        None | Some(AddDropSync::ADD) => RepairMode::Add,
        Some(AddDropSync::DROP) => RepairMode::Drop,
        Some(AddDropSync::SYNC) => RepairMode::Sync,
    };
    repair(catalog.as_ref(), &identifier, &table, mode)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx.ctx())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairMode {
    Add,
    Drop,
    Sync,
}

async fn repair(
    catalog: &dyn Catalog,
    identifier: &Identifier,
    table: &Table,
    mode: RepairMode,
) -> paimon::Result<()> {
    let core_options = CoreOptions::new(table.schema().options());
    let partition_paths = FormatTablePartitionPaths::new(
        table.schema().partition_keys().iter().cloned(),
        core_options.format_table_partition_only_value_in_path(),
    );
    let table_path = table.location();

    if matches!(mode, RepairMode::Drop | RepairMode::Sync) {
        // Discovery treats a missing root as empty. Destructive repair must fail
        // instead, or it could unregister every catalog partition.
        table.file_io().list_status(table_path).await?;
    }

    // Load both views before changing catalog metadata so a listing failure leaves
    // metadata unchanged. Discovery preserves raw directory values such as month=01.
    let discovered_specs = partition_paths
        .discover(
            table.file_io(),
            table_path,
            core_options.partition_default_name(),
        )
        .await?;
    let registered_partitions = catalog.list_partitions(identifier).await?;
    // A partition registered at a location of its own is never discovered under the table
    // directory, so repair leaves it registered.
    let custom_located = registered_partitions
        .iter()
        .filter(|partition| {
            partition
                .options
                .as_ref()
                .is_some_and(|options| options.contains_key("path"))
        })
        .map(|partition| partition_paths.partition_name(&partition.spec))
        .collect::<paimon::Result<HashSet<_>>>()?;

    let discovered_by_name = index_specs_by_name(&partition_paths, discovered_specs)?;
    let registered_by_name = index_specs_by_name(
        &partition_paths,
        registered_partitions
            .into_iter()
            .map(|partition| partition.spec)
            .collect(),
    )?;

    let to_register = if matches!(mode, RepairMode::Add | RepairMode::Sync) {
        discovered_by_name
            .iter()
            .filter(|(name, _)| !registered_by_name.contains_key(*name))
            .map(|(_, spec)| spec.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let to_unregister = if matches!(mode, RepairMode::Drop | RepairMode::Sync) {
        registered_by_name
            .iter()
            .filter(|(name, _)| {
                !discovered_by_name.contains_key(*name) && !custom_located.contains(*name)
            })
            .map(|(_, spec)| spec.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    if !to_register.is_empty() {
        catalog
            .create_partitions(identifier, to_register, true)
            .await?;
    }
    if !to_unregister.is_empty() {
        catalog.drop_partitions(identifier, to_unregister).await?;
    }
    Ok(())
}

fn index_specs_by_name(
    partition_paths: &FormatTablePartitionPaths,
    specs: Vec<HashMap<String, String>>,
) -> paimon::Result<BTreeMap<String, HashMap<String, String>>> {
    specs
        .into_iter()
        .map(|spec| Ok((partition_paths.partition_name(&spec)?, spec)))
        .collect()
}
