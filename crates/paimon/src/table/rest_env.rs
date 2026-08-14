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
use crate::api::GetTableResponse;
use crate::catalog::{Identifier, RESTTokenFileIO};
use crate::common::Options;
use crate::error::Error;
use crate::io::cache::LocalCache;
use crate::io::FileIO;
use crate::spec::{CoreOptions, TableSchema, PATH_OPTION};
use crate::table::snapshot_commit::{RESTSnapshotCommit, SnapshotCommit};
use crate::table::{ObjectTable, Table};
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
    local_cache: Option<Arc<LocalCache>>,
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
    pub(crate) fn new(
        identifier: Identifier,
        uuid: String,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Self {
        Self {
            identifier,
            uuid,
            api,
            options,
            data_token_enabled,
            local_cache,
        }
    }

    #[cfg(test)]
    fn has_local_cache(&self) -> bool {
        self.local_cache.is_some()
    }

    /// Get the REST API client.
    pub fn api(&self) -> &Arc<RESTApi> {
        &self.api
    }

    /// Bracketed by a freshness check: the response names no table, so a drop
    /// and re-create in between would let a replacement's grant serve this one.
    pub(crate) async fn table_query_auth(
        &self,
        branch: &str,
        schema_id: i64,
        select: Option<Vec<String>>,
    ) -> Result<crate::api::AuthTableQueryResponse> {
        self.current_table_checked(schema_id).await?;
        let response = self
            .api
            .auth_table_query(&self.branch_identifier(branch), select)
            .await?;
        self.current_table_checked(schema_id).await?;
        Ok(response)
    }

    /// Asserts nothing about identity: an ordinary table must not inherit a
    /// freshness restriction.
    pub(crate) async fn current_table(&self) -> Result<GetTableResponse> {
        self.api.get_table(&self.identifier).await
    }

    /// Refused unless the name still resolves to the loaded table — a missing
    /// identity too, which checks nothing.
    pub(crate) async fn current_table_checked(&self, schema_id: i64) -> Result<GetTableResponse> {
        let response = self.current_table().await?;
        let name = self.identifier.full_name();
        let drifted = |what: &str, from: String, to: String| crate::Error::DataInvalid {
            message: format!(
                "table '{name}' now resolves to {what} {to}, not the {from} this handle was \
                 loaded with; re-load the table before reading it"
            ),
            source: None,
        };
        match response.id.as_deref() {
            Some(uuid) if uuid == self.uuid => {}
            Some(uuid) => return Err(drifted("uuid", self.uuid.clone(), uuid.to_string())),
            None => {
                return Err(drifted(
                    "uuid",
                    self.uuid.clone(),
                    "nothing the server reports".to_string(),
                ))
            }
        }
        match response.schema_id {
            Some(id) if id == schema_id => Ok(response),
            Some(id) => Err(drifted("schema", schema_id.to_string(), id.to_string())),
            None => Err(drifted(
                "schema",
                schema_id.to_string(),
                "nothing the server reports".to_string(),
            )),
        }
    }

    /// `db.table$branch_<name>`, as Java names a branch. Only the auth call uses it.
    fn branch_identifier(&self, branch: &str) -> Identifier {
        if branch == crate::catalog::DEFAULT_MAIN_BRANCH {
            return self.identifier.clone();
        }
        Identifier::new(
            self.identifier.database(),
            format!(
                "{}{}{}{}",
                self.identifier.object(),
                crate::catalog::SYSTEM_TABLE_SPLITTER,
                crate::catalog::SYSTEM_BRANCH_PREFIX,
                branch
            ),
        )
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
            self.local_cache.clone(),
        )
        .await
    }

    /// Load a REST table and attach a fresh RESTEnv to it.
    pub(crate) async fn load_table(
        identifier: &Identifier,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Result<Table> {
        let response = Self::fetch_table_response(identifier, &api).await?;
        Self::build_table(
            identifier,
            response,
            api,
            options,
            data_token_enabled,
            local_cache,
        )
        .await
    }

    /// Fetch the raw table metadata, mapping REST errors to catalog errors.
    pub(crate) async fn fetch_table_response(
        identifier: &Identifier,
        api: &RESTApi,
    ) -> Result<crate::api::GetTableResponse> {
        api.get_table(identifier)
            .await
            .map_err(|e| map_rest_error_for_table(e, identifier))
    }

    /// Build a Table from an already-fetched response, so routing can
    /// inspect the declared type first.
    pub(crate) async fn build_table(
        identifier: &Identifier,
        response: crate::api::GetTableResponse,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Result<Table> {
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
        // Fail closed: constructed as Paimon, raw `get_table` paths (writes,
        // procedures, time travel) would misread it.
        let declared = CoreOptions::new(schema.options()).table_type()?;
        if declared.requires_table_engine() {
            return Err(Error::Unsupported {
                message: format!(
                    "table '{}' is declared '{declared}' and cannot be read as a Paimon \
                     table; only plain reads through a registered table engine are supported",
                    identifier.full_name()
                ),
            });
        }

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
        validate_catalog_managed_format_table(identifier, &table_schema, is_external)?;

        let uuid = response.id.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing id (uuid)",
                identifier.full_name()
            ),
            source: None,
        })?;

        let file_io = Self::build_file_io(
            identifier,
            &table_path,
            api.clone(),
            &options,
            data_token_enabled,
            is_external,
            local_cache.clone(),
        )
        .await?;

        let rest_env = RESTEnv::new(
            identifier.clone(),
            uuid,
            api,
            options,
            data_token_enabled,
            local_cache,
        );

        Ok(Table::new(
            file_io,
            identifier.clone(),
            table_path,
            table_schema,
            Some(rest_env),
        )
        .with_query_auth_session())
    }

    pub(crate) async fn build_object_table(
        identifier: &Identifier,
        response: crate::api::GetTableResponse,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Result<ObjectTable> {
        let schema = response.schema.ok_or_else(|| Error::DataInvalid {
            message: format!("Table {} response missing schema", identifier.full_name()),
            source: None,
        })?;
        let schema_id = response.schema_id.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing schema_id",
                identifier.full_name()
            ),
            source: None,
        })?;
        let object_path = response
            .path
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .ok_or_else(|| Error::ConfigInvalid {
                message: format!(
                    "Object table '{}' response requires a non-empty path",
                    identifier.full_name()
                ),
            })?
            .to_string();
        let mut schema_options = schema.options().clone();
        schema_options.insert(PATH_OPTION.to_string(), object_path.clone());
        let table_schema = TableSchema::new(schema_id, &schema).copy_with_options(schema_options);
        let is_external = response.is_external.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing is_external",
                identifier.full_name()
            ),
            source: None,
        })?;

        let file_io = Self::build_file_io(
            identifier,
            &object_path,
            api,
            &options,
            data_token_enabled,
            is_external,
            local_cache,
        )
        .await?;

        ObjectTable::try_new(file_io, identifier.clone(), &table_schema)
    }

    async fn build_file_io(
        identifier: &Identifier,
        path: &str,
        api: Arc<RESTApi>,
        options: &Options,
        data_token_enabled: bool,
        is_external: bool,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Result<FileIO> {
        if data_token_enabled && !is_external {
            return Arc::new(RESTTokenFileIO::new(
                identifier.clone(),
                path.to_string(),
                options.clone(),
                api,
                local_cache,
            ))
            .build_file_io()
            .await;
        }

        let mut builder = FileIO::from_path(path)?.with_props(options.to_map());
        if let Some(local_cache) = local_cache {
            builder = builder.with_local_cache(local_cache);
        }
        builder.build()
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

/// Refuse a Format Table that asks for catalog-managed partitions it cannot have: an engine
/// implementation reads the table directory itself, and only an internal table's partitions
/// belong to the catalog.
///
/// Mirrors Java `CatalogUtils.validateCatalogManagedFormatTablePartitions`.
fn validate_catalog_managed_format_table(
    identifier: &Identifier,
    table_schema: &TableSchema,
    is_external: bool,
) -> Result<()> {
    let options = CoreOptions::new(table_schema.options());
    if !options.is_format_table() || !options.partitioned_table_in_metastore() {
        return Ok(());
    }
    if options.format_table_implementation_is_engine() {
        return Err(Error::DataInvalid {
            message: format!(
                "Format Table {} cannot set metastore.partitioned-table=true when \
                 format-table.implementation=engine",
                identifier.full_name()
            ),
            source: None,
        });
    }
    if is_external {
        return Err(Error::DataInvalid {
            message: format!(
                "Catalog-managed partitions require an internal Format Table, but {} is external",
                identifier.full_name()
            ),
            source: None,
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CatalogOptions;
    use crate::io::cache::create_local_cache;

    #[tokio::test]
    async fn test_rest_env_clones_catalog_local_cache() {
        let cache_directory = tempfile::tempdir().unwrap();
        let mut options = Options::new();
        options.set(CatalogOptions::URI, "http://localhost:1");
        options.set(CatalogOptions::WAREHOUSE, "test-warehouse");
        options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
        options.set(CatalogOptions::TOKEN, "test-token");
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        options.set(
            CatalogOptions::LOCAL_CACHE_DIR,
            cache_directory.path().to_string_lossy(),
        );
        let local_cache = create_local_cache(&options).unwrap();
        let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());

        let rest_env = RESTEnv::new(
            Identifier::new("database", "table"),
            "uuid".to_string(),
            api,
            options,
            false,
            local_cache,
        );

        assert!(rest_env.has_local_cache());
        assert!(rest_env.clone().has_local_cache());
    }
}
