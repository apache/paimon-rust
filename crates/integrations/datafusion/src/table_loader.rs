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

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use paimon::catalog::{Catalog, Identifier};
use paimon::table::Table;

use crate::error::to_datafusion_error;

/// [`Catalog::get_table`] for paths that need a `Table` rather than a
/// [`paimon::catalog::LoadedTable`], rejecting an engine-served declared
/// type: `get_table` is a trait method, so a catalog outside this repository
/// can return a table for any type.
pub(crate) async fn get_paimon_table(
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
) -> DFResult<Table> {
    let table = catalog
        .get_table(identifier)
        .await
        .map_err(to_datafusion_error)?;
    ensure_paimon_served(&table, identifier)?;
    Ok(table)
}

/// The check from [`get_paimon_table`], for callers that already hold the
/// table or special-case the `get_table` error.
pub(crate) fn ensure_paimon_served(table: &Table, identifier: &Identifier) -> DFResult<()> {
    let declared = paimon::spec::CoreOptions::new(table.schema().options())
        .table_type()
        .map_err(to_datafusion_error)?;
    if declared.requires_table_engine() {
        return Err(DataFusionError::Plan(format!(
            "table '{}' is declared '{}' and cannot be read as a Paimon table",
            identifier.full_name(),
            declared
        )));
    }
    Ok(())
}

pub(crate) async fn load_table_for_read(
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
) -> DFResult<(Table, Identifier, Option<String>)> {
    let parsed = identifier
        .parsed_object_name()
        .map_err(to_datafusion_error)?;
    let base_identifier = Identifier::new(
        identifier.database().to_string(),
        parsed.table().to_string(),
    );
    let mut table = match catalog.load_table(&base_identifier).await {
        Ok(paimon::catalog::LoadedTable::Paimon(table)) => *table,
        Ok(paimon::catalog::LoadedTable::Object(_)) => {
            // The search UDTFs land here too, so the message names the table
            // rather than what the caller asked of it.
            return Err(DataFusionError::Plan(format!(
                "table '{}' is declared 'object-table' and cannot be read as a Paimon table",
                base_identifier.full_name()
            )));
        }
        Ok(paimon::catalog::LoadedTable::External(external)) => {
            return Err(DataFusionError::Plan(format!(
                "table '{}' is declared '{}' and cannot be read as a Paimon table",
                base_identifier.full_name(),
                external.declared()
            )));
        }
        // `LoadedTable` is non_exhaustive: a variant added upstream is not a
        // Paimon table until this path says how to read one.
        Ok(_) => {
            return Err(DataFusionError::Plan(format!(
                "table '{}' cannot be read as a Paimon table",
                base_identifier.full_name()
            )));
        }
        Err(err) => return Err(to_datafusion_error(err)),
    };
    let system_table = parsed.system_table().map(str::to_string);
    if let Some(branch) = parsed.branch() {
        let is_branches_table = system_table
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case("branches"));
        if is_branches_table {
            return Ok((table, base_identifier, system_table));
        }
        table = table
            .copy_with_branch(branch)
            .await
            .map_err(to_datafusion_error)?;
    }
    Ok((table, base_identifier, system_table))
}

pub(crate) async fn load_data_table_for_read(
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
    caller: &str,
) -> DFResult<Table> {
    let (table, _, system_table) = load_table_for_read(catalog, identifier).await?;
    if system_table.is_some() {
        return Err(DataFusionError::Plan(format!(
            "{caller} requires a data table"
        )));
    }
    Ok(table)
}
