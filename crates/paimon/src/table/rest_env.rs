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

//! REST environment for REST-backed table operations.

use crate::api::rest_api::RESTApi;
use crate::api::rest_error::RestError;
use crate::catalog::{Identifier, RESTTokenFileIO};
use crate::common::Options;
use crate::error::Error;
use crate::io::FileIO;
use crate::spec::{CoreOptions, TableSchema, PATH_OPTION};
use crate::table::snapshot_commit::{RESTSnapshotCommit, SnapshotCommit};
use crate::table::Table;
use crate::Result;
use std::sync::Arc;

/// REST environment that holds the REST API client, identifier, and uuid
/// needed to create a `RESTSnapshotCommit`.
#[derive(Clone)]
pub struct RESTEnv {
    identifier: Identifier,
    uuid: String,
    api: Arc<RESTApi>,
    options: Options,
    data_token_enabled: bool,
}

impl std::fmt::Debug for RESTEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RESTEnv")
            .field("identifier", &self.identifier)
            .field("uuid", &self.uuid)
            .finish()
    }
}

impl RESTEnv {
    /// Create a new RESTEnv.
    pub fn new(
        identifier: Identifier,
        uuid: String,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
    ) -> Self {
        Self {
            identifier,
            uuid,
            api,
            options,
            data_token_enabled,
        }
    }

    /// Get the REST API client.
    pub fn api(&self) -> &Arc<RESTApi> {
        &self.api
    }

    /// Get the table identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Load a table through the same REST catalog environment.
    pub async fn get_table(&self, identifier: &Identifier) -> Result<Table> {
        Self::load_table(
            identifier,
            self.api.clone(),
            self.options.clone(),
            self.data_token_enabled,
        )
        .await
    }

    /// Load a REST table and attach a fresh RESTEnv to it.
    pub(crate) async fn load_table(
        identifier: &Identifier,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
    ) -> Result<Table> {
        let response = api
            .get_table(identifier)
            .await
            .map_err(|e| map_rest_error_for_table(e, identifier))?;

        let schema = response.schema.ok_or_else(|| Error::DataInvalid {
            message: format!("Table {} response missing schema", identifier.full_name()),
            source: None,
        })?;

        let table_path = response.path.ok_or_else(|| Error::DataInvalid {
            message: format!("Table {} response missing path", identifier.full_name()),
            source: None,
        })?;

        let schema_id = response.schema_id.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing schema_id",
                identifier.full_name()
            ),
            source: None,
        })?;
        let mut table_schema = TableSchema::new(schema_id, &schema);
        if CoreOptions::new(table_schema.options()).is_format_table() {
            table_schema = table_schema.copy_with_options(std::collections::HashMap::from([(
                PATH_OPTION.to_string(),
                table_path.clone(),
            )]));
        }

        let is_external = response.is_external.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing is_external",
                identifier.full_name()
            ),
            source: None,
        })?;

        let uuid = response.id.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing id (uuid)",
                identifier.full_name()
            ),
            source: None,
        })?;

        let file_io = if data_token_enabled && !is_external {
            RESTTokenFileIO::new(identifier.clone(), table_path.clone(), options.clone())
                .build_file_io()
                .await?
        } else {
            let mut builder = FileIO::from_path(&table_path)?;
            builder = builder.with_props(options.to_map());
            builder.build()?
        };

        let rest_env = RESTEnv::new(identifier.clone(), uuid, api, options, data_token_enabled);

        Ok(Table::new(
            file_io,
            identifier.clone(),
            table_path,
            table_schema,
            Some(rest_env),
        ))
    }

    /// Create a `RESTSnapshotCommit` from this environment.
    pub fn snapshot_commit(&self) -> Arc<dyn SnapshotCommit> {
        Arc::new(RESTSnapshotCommit::new(
            self.api.clone(),
            self.identifier.clone(),
            self.uuid.clone(),
        ))
    }
}

fn map_rest_error_for_table(err: Error, identifier: &Identifier) -> Error {
    match err {
        Error::RestApi {
            source: RestError::NoSuchResource { .. },
        } => Error::TableNotExist {
            full_name: identifier.full_name(),
        },
        Error::RestApi {
            source: RestError::AlreadyExists { .. },
        } => Error::TableAlreadyExist {
            full_name: identifier.full_name(),
        },
        other => other,
    }
}
