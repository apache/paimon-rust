// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::Path;
use std::time::Instant;

use datafusion::arrow::array::UInt64Array;
use datafusion::prelude::ParquetReadOptions;
use paimon::arrow::arrow_to_paimon_type;
use paimon::catalog::Identifier;
use paimon::spec::Schema;
use paimon::{Catalog, Error as PaimonError};
use serde::{Deserialize, Serialize};

use crate::context::CatalogSession;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingTablePolicy {
    Error,
    Skip,
    Overwrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadStatus {
    Loaded,
    Skipped,
    Overwritten,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableLoadResult {
    pub table: String,
    pub status: LoadStatus,
    pub rows: u64,
    pub elapsed_ms: u128,
}

pub async fn load_parquet_table(
    session: &CatalogSession,
    data_root: &Path,
    table_name: &str,
    existing_policy: ExistingTablePolicy,
) -> Result<TableLoadResult, BoxError> {
    let started = Instant::now();
    let identifier = Identifier::new(&session.database, table_name);
    let exists = match session.catalog.get_table(&identifier).await {
        Ok(_) => true,
        Err(PaimonError::TableNotExist { .. }) => false,
        Err(error) => return Err(error.into()),
    };

    if exists && existing_policy == ExistingTablePolicy::Skip {
        return Ok(TableLoadResult {
            table: table_name.to_string(),
            status: LoadStatus::Skipped,
            rows: 0,
            elapsed_ms: started.elapsed().as_millis(),
        });
    }
    if exists && existing_policy == ExistingTablePolicy::Error {
        return Err(PaimonError::TableAlreadyExist {
            full_name: identifier.full_name(),
        }
        .into());
    }

    let source_path = data_root.join(format!("{table_name}.parquet"));
    let source_path = source_path
        .to_str()
        .ok_or_else(|| format!("source path is not valid UTF-8: {}", source_path.display()))?;
    let source = session
        .sql
        .ctx()
        .read_parquet(source_path, ParquetReadOptions::default())
        .await?;
    let arrow_schema = source.schema().inner().clone();

    if !exists {
        let mut schema = Schema::builder();
        for field in arrow_schema.fields() {
            schema = schema.column(
                field.name(),
                arrow_to_paimon_type(field.data_type(), field.is_nullable())?,
            );
        }
        session
            .catalog
            .create_table(&identifier, schema.build()?, false)
            .await?;
    }

    let source_name = format!("__tpcds_source_{table_name}");
    let source_reference = format!(
        "{}.{}.{}",
        quote_identifier(&session.catalog_name),
        quote_identifier(&session.database),
        quote_identifier(&source_name)
    );
    if session.sql.temp_table_exist(source_reference.as_str())? {
        session
            .sql
            .deregister_temp_table(source_reference.as_str())?;
    }
    session
        .sql
        .register_temp_table(source_reference.as_str(), source.into_view())?;

    let target_reference = format!(
        "{}.{}.{}",
        quote_identifier(&session.catalog_name),
        quote_identifier(&session.database),
        quote_identifier(table_name)
    );
    let operation = if exists {
        "INSERT OVERWRITE"
    } else {
        "INSERT INTO"
    };
    let load_result = session
        .sql
        .sql(&format!(
            "{operation} {target_reference} SELECT * FROM {source_reference}"
        ))
        .await;
    let batches = match load_result {
        Ok(frame) => frame.collect().await,
        Err(error) => Err(error),
    };
    let _ = session.sql.deregister_temp_table(source_reference.as_str());
    let batches = batches?;
    let rows = batches
        .first()
        .and_then(|batch| batch.column(0).as_any().downcast_ref::<UInt64Array>())
        .map(|counts| counts.value(0))
        .ok_or("DataFusion INSERT did not return a UInt64 row count")?;

    Ok(TableLoadResult {
        table: table_name.to_string(),
        status: if exists {
            LoadStatus::Overwritten
        } else {
            LoadStatus::Loaded
        },
        rows,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
