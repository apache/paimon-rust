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

//! [`TableProviderFactory`] implementation for creating Paimon tables via
//! `CREATE EXTERNAL TABLE`.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::CreateExternalTable;
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::Schema;

use crate::error::to_datafusion_error;
use crate::table::PaimonTableProvider;
use paimon::arrow::arrow_to_paimon_type;

/// A [`TableProviderFactory`] that creates Paimon tables.
///
/// Register with:
/// ```ignore
/// ctx.state_mut().table_factories_mut()
///     .insert("PAIMON".to_string(), Arc::new(PaimonTableFactory::new(catalog)));
/// ```
///
/// Then use:
/// ```sql
/// CREATE EXTERNAL TABLE paimon.my_db.my_table (
///   id INT NOT NULL,
///   name STRING,
///   dt STRING,
///   PRIMARY KEY (id, dt)
/// ) PARTITIONED BY (dt)
/// WITH ('bucket' = '2');
/// ```
pub struct PaimonTableFactory {
    catalog: Arc<dyn Catalog>,
}

impl Debug for PaimonTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonTableFactory").finish()
    }
}

impl PaimonTableFactory {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl TableProviderFactory for PaimonTableFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DFResult<Arc<dyn TableProvider>> {
        if !cmd.location.is_empty() {
            return Err(DataFusionError::Plan(
                "LOCATION is not supported for Paimon tables. Table path is determined by the catalog warehouse.".to_string(),
            ));
        }

        let identifier = resolve_identifier(&cmd.name)?;

        // Build Paimon schema from the CREATE EXTERNAL TABLE command.
        let arrow_schema = cmd.schema.as_arrow();
        let mut builder = Schema::builder();

        for field in arrow_schema.fields() {
            let paimon_type = arrow_to_paimon_type(field.data_type(), field.is_nullable())
                .map_err(to_datafusion_error)?;
            builder = builder.column(field.name().clone(), paimon_type);
        }

        if !cmd.table_partition_cols.is_empty() {
            builder = builder.partition_keys(cmd.table_partition_cols.clone());
        }

        // Pass all OPTIONS through to Paimon (includes 'bucket', etc.).
        // DataFusion prefixes options with "format." — strip that prefix for Paimon.
        for (k, v) in &cmd.options {
            let key = k.strip_prefix("format.").unwrap_or(k);
            builder = builder.option(key.to_string(), v.clone());
        }

        let schema = builder.build().map_err(to_datafusion_error)?;

        self.catalog
            .create_table(&identifier, schema, cmd.if_not_exists)
            .await
            .map_err(to_datafusion_error)?;

        // Return the newly created table as a provider.
        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;
        let provider = PaimonTableProvider::try_new(table)?;
        Ok(Arc::new(provider))
    }
}

/// Extract a Paimon [`Identifier`] (database, table) from a DataFusion [`TableReference`].
fn resolve_identifier(name: &TableReference) -> DFResult<Identifier> {
    match name {
        TableReference::Full {
            schema, table, ..
        } => Ok(Identifier::new(schema.to_string(), table.to_string())),
        TableReference::Partial { schema, table } => {
            Ok(Identifier::new(schema.to_string(), table.to_string()))
        }
        TableReference::Bare { table } => Err(DataFusionError::Plan(format!(
            "CREATE EXTERNAL TABLE requires a fully qualified name (catalog.database.table or database.table), got: {table}"
        ))),
    }
}
