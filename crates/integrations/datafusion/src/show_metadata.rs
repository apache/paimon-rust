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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{col, lit, DataFrame, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    ShowStatementFilter, ShowStatementFilterPosition, ShowStatementOptions, Statement,
};
use paimon::catalog::Catalog;

pub(crate) async fn execute(
    ctx: &SessionContext,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    statement: &Statement,
) -> Result<DataFrame> {
    let (options, invalid_modifiers, kind) = match statement {
        Statement::ShowCatalogs {
            terse,
            history,
            show_options,
        } => (show_options, *terse || *history, "SHOW CATALOGS"),
        Statement::ShowDatabases {
            terse,
            history,
            show_options,
        }
        | Statement::ShowSchemas {
            terse,
            history,
            show_options,
        } => (show_options, *terse || *history, "SHOW DATABASES"),
        Statement::ShowTables {
            terse,
            history,
            extended,
            full,
            external,
            show_options,
        } => (
            show_options,
            *terse || *history || *extended || *full || *external,
            "SHOW TABLES",
        ),
        _ => unreachable!("only metadata SHOW statements are dispatched here"),
    };
    if invalid_modifiers
        || options.starts_with.is_some()
        || options.limit.is_some()
        || options.limit_from.is_some()
    {
        return Err(DataFusionError::Plan(format!(
            "{kind} options are not supported"
        )));
    }
    let state = ctx.state();
    let config = &state.config_options().catalog;
    let normalizer = IdentNormalizer::new(ctx.enable_ident_normalization());
    let scope = options
        .show_in
        .as_ref()
        .map(|scope| {
            let name = scope
                .parent_name
                .as_ref()
                .ok_or_else(|| DataFusionError::Plan("SHOW IN requires a name".into()))?;
            if scope.parent_type.is_some() {
                return Err(DataFusionError::Plan(
                    "typed SHOW IN scopes are not supported".into(),
                ));
            }
            name.0
                .iter()
                .map(|part| {
                    part.as_ident()
                        .map(|id| normalizer.normalize(id.clone()))
                        .ok_or_else(|| DataFusionError::Plan("SHOW IN requires identifiers".into()))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    match statement {
        Statement::ShowCatalogs { .. } => {
            if scope.is_some() {
                return Err(DataFusionError::Plan(
                    "SHOW CATALOGS does not accept IN".into(),
                ));
            }
            filter(
                names_frame(ctx, "catalog_name", ctx.catalog_names())?,
                options,
                "catalog_name",
            )
        }
        Statement::ShowDatabases { .. } | Statement::ShowSchemas { .. } => {
            let catalog_name = match scope.as_deref() {
                None => &config.default_catalog,
                Some([catalog]) => catalog,
                _ => {
                    return Err(DataFusionError::Plan(
                        "SHOW DATABASES IN requires one catalog name".into(),
                    ))
                }
            };
            let names = if let Some(catalog) = catalogs.get(catalog_name) {
                catalog
                    .list_databases()
                    .await
                    .map_err(crate::to_datafusion_error)?
            } else {
                ctx.catalog(catalog_name)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("Unknown catalog '{catalog_name}'"))
                    })?
                    .schema_names()
            };
            filter(
                names_frame(ctx, "database_name", names)?,
                options,
                "database_name",
            )
        }
        Statement::ShowTables { .. } => {
            let mut frame = ctx.table("information_schema.tables").await?;
            // Preserve unqualified SHOW TABLES across all catalogs. Scoped or filtered
            // listings default to the current catalog/database.
            if scope.is_some() || options.filter_position.is_some() {
                let (catalog_name, database_name) = match scope.as_deref() {
                    None => (&config.default_catalog, &config.default_schema),
                    Some([database]) => (&config.default_catalog, database),
                    Some([catalog, database]) => (catalog, database),
                    _ => {
                        return Err(DataFusionError::Plan(
                            "SHOW TABLES IN requires database or catalog.database".into(),
                        ))
                    }
                };
                if let Some(catalog) = catalogs.get(catalog_name) {
                    // Validate without loading table data or swallowing permissions.
                    catalog
                        .get_database(database_name)
                        .await
                        .map_err(crate::to_datafusion_error)?;
                } else if ctx
                    .catalog(catalog_name)
                    .and_then(|c| c.schema(database_name))
                    .is_none()
                {
                    return Err(DataFusionError::Plan(format!(
                        "Unknown namespace '{catalog_name}.{database_name}'"
                    )));
                }
                frame = frame
                    .filter(col("table_catalog").eq(lit(catalog_name.clone())))?
                    .filter(col("table_schema").eq(lit(database_name.clone())))?;
            }
            filter(frame, options, "table_name")?.sort(vec![
                col("table_catalog").sort(true, true),
                col("table_schema").sort(true, true),
                col("table_name").sort(true, true),
            ])
        }
        _ => unreachable!(),
    }
}

fn names_frame(ctx: &SessionContext, name: &str, mut values: Vec<String>) -> Result<DataFrame> {
    values.sort_unstable();
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
    ctx.read_batch(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(values))],
    )?)
}

fn filter(frame: DataFrame, options: &ShowStatementOptions, name: &str) -> Result<DataFrame> {
    match options.filter_position.as_ref() {
        None => Ok(frame),
        Some(
            ShowStatementFilterPosition::Infix(filter)
            | ShowStatementFilterPosition::Suffix(filter),
        ) => match filter {
            ShowStatementFilter::Like(pattern) => {
                frame.filter(col(name).like(lit(pattern.clone())))
            }
            ShowStatementFilter::ILike(pattern) => {
                frame.filter(col(name).ilike(lit(pattern.clone())))
            }
            _ => Err(DataFusionError::Plan(
                "SHOW supports only LIKE and ILIKE filters".into(),
            )),
        },
    }
}
