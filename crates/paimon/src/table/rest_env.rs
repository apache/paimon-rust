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
use crate::catalog::{FusePathResolver, Identifier, RESTToken, RESTTokenFileIO};
use crate::common::{CatalogOptions, Options};
use crate::error::Error;
use crate::io::cache::{create_local_cache_with_namespace, LocalCache};
use crate::io::{FileIO, FileIOCacheContext};
use crate::spec::{CoreOptions, TableSchema, PATH_OPTION};
use crate::table::snapshot_commit::{RESTSnapshotCommit, SnapshotCommit};
use crate::table::{ObjectTable, Table};
use crate::Result;
use std::sync::Arc;

impl Table {
    /// Reuse the matching REST response and merged catalog options without config/get-table requests.
    /// Preserves REST snapshots, credential refresh and local caching.
    pub async fn from_rest_response(
        identifier: Identifier,
        response: crate::api::GetTableResponse,
        rest_options: Options,
    ) -> Result<Self> {
        Self::from_rest_response_with_token(identifier, response, rest_options, None).await
    }

    /// As `from_rest_response`, but reuse a matching table data token already
    /// fetched by the caller. Rust will refresh it from REST after expiry.
    pub async fn from_rest_response_with_token(
        identifier: Identifier,
        response: crate::api::GetTableResponse,
        rest_options: Options,
        initial_token: Option<RESTToken>,
    ) -> Result<Self> {
        Self::from_rest_response_with_io(identifier, response, rest_options, initial_token, None)
            .await
    }

    /// Reuse REST publication and authorization with an already resolved FileIO.
    /// The caller owns storage routing and credential refresh. No built-in
    /// storage backend or table data token is initialized for this FileIO.
    pub async fn from_rest_response_with_file_io(
        identifier: Identifier,
        response: crate::api::GetTableResponse,
        rest_options: Options,
        file_io: FileIO,
    ) -> Result<Self> {
        Self::from_rest_response_with_io(identifier, response, rest_options, None, Some(file_io))
            .await
    }

    async fn from_rest_response_with_io(
        identifier: Identifier,
        response: crate::api::GetTableResponse,
        rest_options: Options,
        initial_token: Option<RESTToken>,
        file_io: Option<FileIO>,
    ) -> Result<Self> {
        identifier.validate()?;
        // Reject a stale or misrouted response before initializing auth or local-cache resources.
        response_identifier(&identifier, &response)?;
        rest_options
            .get(CatalogOptions::WAREHOUSE)
            .ok_or_else(|| RestError::BadRequest {
                message: format!("Missing required option: {}", CatalogOptions::WAREHOUSE),
            })?;
        let api = Arc::new(RESTApi::new(rest_options.clone(), false).await?);
        let data_token_enabled = api
            .options()
            .get(CatalogOptions::DATA_TOKEN_ENABLED)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let local_cache = create_local_cache_with_namespace(&rest_options, api.options())?;
        let file_io_cache = FileIOCacheContext::from_props(api.options().to_map())?;
        let fuse_resolver = Arc::new(FusePathResolver::new(api.options()));
        RESTEnv::build_table_with_token(
            &identifier,
            response,
            api,
            rest_options,
            data_token_enabled,
            local_cache,
            file_io_cache,
            initial_token,
            fuse_resolver,
            file_io,
        )
        .await
    }
}

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
    file_io_cache: FileIOCacheContext,
    fuse_resolver: Arc<FusePathResolver>,
    file_io_override: Option<FileIO>,
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
        file_io_cache: FileIOCacheContext,
    ) -> Self {
        let fuse_resolver = Arc::new(FusePathResolver::new(api.options()));
        Self {
            identifier,
            uuid,
            api,
            options,
            data_token_enabled,
            local_cache,
            file_io_cache,
            fuse_resolver,
            file_io_override: None,
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

    /// Bind the response to a stable catalog entity and latest rule schema.
    /// Historical views use their own schema only when executing these rules.
    pub(crate) async fn table_query_auth(
        &self,
        schema_id: i64,
        fields: &[crate::spec::DataField],
        select: Option<Vec<String>>,
        query_auth: Option<&dyn super::TableQueryAuth>,
    ) -> Result<(crate::api::AuthTableQueryResponse, TableSchema)> {
        let current = self.current_table_identity_checked().await?;
        // A catalog-loaded handle can predate a normal schema evolution, just
        // as a historical view does. Reusing an id for different fields is not
        // a legitimate evolution and must still fail closed.
        if current.schema_id == Some(schema_id) {
            self.check_schema(&current, schema_id, fields)?;
        }
        let schema = current.schema.as_ref().ok_or_else(|| Error::DataInvalid {
            message: "REST query authorization requires the current schema".into(),
            source: None,
        })?;
        let schema = TableSchema::new(
            current.schema_id.ok_or_else(|| Error::DataInvalid {
                message: "REST query authorization requires the current schema id".into(),
                source: None,
            })?,
            schema,
        );
        let response = match query_auth {
            Some(query_auth) => query_auth.auth(select).await?.unwrap_or_default(),
            None => self.api.auth_table_query(&self.identifier, select).await?,
        };
        self.current_table_checked(schema.id(), schema.fields())
            .await?;
        Ok((response, schema))
    }

    /// Refused unless the name still resolves to the loaded table, a missing
    /// identity included. Asserts nothing on its own.
    pub(crate) async fn current_table_checked(
        &self,
        schema_id: i64,
        fields: &[crate::spec::DataField],
    ) -> Result<GetTableResponse> {
        let response = self.current_table_identity_checked().await?;
        self.check_schema(&response, schema_id, fields)?;
        Ok(response)
    }

    fn check_schema(
        &self,
        response: &GetTableResponse,
        schema_id: i64,
        fields: &[crate::spec::DataField],
    ) -> Result<()> {
        let name = self.identifier.full_name();
        let same = |what: &str, loaded: String, now: Option<String>| match now {
            Some(now) if now == loaded => Ok(()),
            now => Err(crate::Error::DataInvalid {
                message: format!(
                    "table '{name}' now resolves to {what} {}, not the {loaded} this handle was \
                     loaded with; re-load the table before reading it",
                    now.as_deref().unwrap_or("nothing the server reports")
                ),
                source: None,
            }),
        };
        same(
            "schema",
            schema_id.to_string(),
            response.schema_id.map(|id| id.to_string()),
        )?;
        // An id is not the schema: the columns the server rules on are compared too.
        let key =
            |f: &crate::spec::DataField| (f.id(), f.name().to_string(), f.data_type().clone());
        let served: Vec<_> = response
            .schema
            .as_ref()
            .map(|schema| schema.fields().iter().map(key).collect())
            .unwrap_or_default();
        if served != fields.iter().map(key).collect::<Vec<_>>() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "table '{name}' serves other columns than this handle carries under schema \
                     {schema_id}; re-load the table before reading it"
                ),
                source: None,
            });
        }
        Ok(())
    }

    /// Refused unless the identifier still resolves to this catalog entity.
    pub(crate) async fn current_table_identity_checked(&self) -> Result<GetTableResponse> {
        let response = self.api.get_table(&self.identifier).await?;
        let name = self.identifier.full_name();
        match response.id.as_deref() {
            Some(uuid) if uuid == self.uuid => {}
            uuid => {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "table '{name}' now resolves to uuid {}, not the {} this handle was loaded \
                         with; re-load the table before reading it",
                        uuid.unwrap_or("nothing the server reports"),
                        self.uuid
                    ),
                    source: None,
                });
            }
        }
        Ok(response)
    }

    /// Get the table identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Catalog identity used to reject commits against a replaced table.
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    /// Attach Java's outermost table identity to dependency requests. The
    /// dependency client is separate so ordinary requests retain their headers.
    pub(crate) async fn for_dependency_reads(&self) -> Result<Self> {
        let key = format!("{}{}", RESTApi::HEADER_PREFIX, RESTApi::READ_VIA_HEADER);
        let mut options = self.api.options().clone();
        if options.get(&key).is_some() {
            return Ok(self.clone());
        }
        let identifier =
            serde_json::to_string(&self.identifier).map_err(|error| Error::DataInvalid {
                message: "Cannot serialize dependency read identifier".to_string(),
                source: Some(Box::new(error)),
            })?;
        options.set(
            key,
            crate::api::rest_util::RESTUtil::encode_string(&identifier),
        );
        let api = Arc::new(RESTApi::new(options.clone(), false).await?);
        Ok(Self {
            api,
            options,
            ..self.clone()
        })
    }

    /// Load a table through the same REST catalog environment.
    pub async fn get_table(&self, identifier: &Identifier) -> Result<Table> {
        // A resolved FileIO belongs to this table, including its branches.
        // Dependency tables must resolve their own routing and credentials.
        if self.file_io_override.is_some()
            && identifier.database() == self.identifier.database()
            && identifier.table_name()? == self.identifier.table_name()?
        {
            let response = Self::fetch_table_response(identifier, &self.api).await?;
            if response.id.as_deref() != Some(self.uuid.as_str()) {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Table {} identity changed; reload it before reusing its FileIO",
                        identifier.full_name()
                    ),
                    source: None,
                });
            }
            return Self::build_table_with_token(
                identifier,
                response,
                self.api.clone(),
                self.options.clone(),
                self.data_token_enabled,
                self.local_cache.clone(),
                self.file_io_cache.clone(),
                None,
                self.fuse_resolver.clone(),
                self.file_io_override.clone(),
            )
            .await;
        }
        Self::load_table(
            identifier,
            self.api.clone(),
            self.options.clone(),
            self.data_token_enabled,
            self.local_cache.clone(),
            self.file_io_cache.clone(),
            self.fuse_resolver.clone(),
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
        file_io_cache: FileIOCacheContext,
        fuse_resolver: Arc<FusePathResolver>,
    ) -> Result<Table> {
        let response = Self::fetch_table_response(identifier, &api).await?;
        Self::build_table(
            identifier,
            response,
            api,
            options,
            data_token_enabled,
            local_cache,
            file_io_cache,
            fuse_resolver,
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn build_table(
        identifier: &Identifier,
        response: crate::api::GetTableResponse,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
        local_cache: Option<Arc<LocalCache>>,
        file_io_cache: FileIOCacheContext,
        fuse_resolver: Arc<FusePathResolver>,
    ) -> Result<Table> {
        Self::build_table_with_token(
            identifier,
            response,
            api,
            options,
            data_token_enabled,
            local_cache,
            file_io_cache,
            None,
            fuse_resolver,
            None,
        )
        .await
    }

    // Keep the REST construction inputs explicit, including the optional initial token.
    #[allow(clippy::too_many_arguments)]
    async fn build_table_with_token(
        identifier: &Identifier,
        response: crate::api::GetTableResponse,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
        local_cache: Option<Arc<LocalCache>>,
        file_io_cache: FileIOCacheContext,
        initial_token: Option<RESTToken>,
        fuse_resolver: Arc<FusePathResolver>,
        file_io_override: Option<FileIO>,
    ) -> Result<Table> {
        let identifier = response_identifier(identifier, &response)?;
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
        table_schema.validate_resolved_structure()?;

        let is_external = response.is_external.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing is_external",
                identifier.full_name()
            ),
            source: None,
        })?;
        validate_catalog_managed_format_table(&identifier, &table_schema, is_external)?;

        let uuid = response.id.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing id (uuid)",
                identifier.full_name()
            ),
            source: None,
        })?;

        let file_io = match file_io_override.clone() {
            Some(file_io) => file_io,
            None => {
                Self::build_file_io_with_token(
                    &identifier,
                    &table_path,
                    api.clone(),
                    &options,
                    data_token_enabled && !is_external,
                    local_cache.clone(),
                    file_io_cache.clone(),
                    initial_token,
                    (!is_external).then_some(fuse_resolver.as_ref()),
                )
                .await?
            }
        };

        let mut rest_env = RESTEnv::new(
            identifier.clone(),
            uuid,
            api,
            options,
            data_token_enabled,
            local_cache,
            file_io_cache,
        );
        rest_env.fuse_resolver = fuse_resolver;
        rest_env.file_io_override = file_io_override;
        let parsed_identifier = identifier.parsed_object_name()?;
        let branch = parsed_identifier.branch_or_default().to_string();
        let branch_reference = parsed_identifier.branch().is_some();
        let table = Table::new(
            file_io,
            identifier,
            table_path,
            table_schema,
            Some(rest_env),
        );

        // Minted after the schema-replacing copy, which drops any session.
        let mut table = table.copy_with_resolved_schema(table.schema().clone(), &branch)?;
        table.branch_reference = branch_reference;
        Ok(table.with_query_auth_session())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn build_object_table(
        identifier: &Identifier,
        response: crate::api::GetTableResponse,
        api: Arc<RESTApi>,
        options: Options,
        data_token_enabled: bool,
        local_cache: Option<Arc<LocalCache>>,
        file_io_cache: FileIOCacheContext,
        fuse_resolver: Arc<FusePathResolver>,
    ) -> Result<ObjectTable> {
        let identifier = response_identifier(identifier, &response)?;
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
        table_schema.validate_resolved_structure()?;
        let is_external = response.is_external.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing is_external",
                identifier.full_name()
            ),
            source: None,
        })?;

        let file_io = Self::build_file_io(
            &identifier,
            &object_path,
            api,
            &options,
            data_token_enabled && !is_external,
            local_cache,
            file_io_cache,
            (!is_external).then_some(fuse_resolver.as_ref()),
        )
        .await?;

        ObjectTable::try_new(file_io, identifier, &table_schema)
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_file_io(
        identifier: &Identifier,
        path: &str,
        api: Arc<RESTApi>,
        options: &Options,
        use_data_token: bool,
        local_cache: Option<Arc<LocalCache>>,
        file_io_cache: FileIOCacheContext,
        fuse_resolver: Option<&FusePathResolver>,
    ) -> Result<FileIO> {
        Self::build_file_io_with_token(
            identifier,
            path,
            api,
            options,
            use_data_token,
            local_cache,
            file_io_cache,
            None,
            fuse_resolver,
        )
        .await
    }

    // Match the existing FileIO builder while passing through the optional initial token.
    #[allow(clippy::too_many_arguments)]
    async fn build_file_io_with_token(
        identifier: &Identifier,
        path: &str,
        api: Arc<RESTApi>,
        options: &Options,
        use_data_token: bool,
        local_cache: Option<Arc<LocalCache>>,
        file_io_cache: FileIOCacheContext,
        initial_token: Option<RESTToken>,
        fuse_resolver: Option<&FusePathResolver>,
    ) -> Result<FileIO> {
        if let Some(fuse) = fuse_resolver {
            if let Some(file_io) = fuse
                .file_io(
                    path,
                    identifier,
                    &api,
                    local_cache.clone(),
                    file_io_cache.clone(),
                )
                .await?
            {
                return Ok(file_io);
            }
        }
        if use_data_token {
            let mut token_file_io = RESTTokenFileIO::new(
                identifier.clone(),
                path.to_string(),
                options.clone(),
                api,
                local_cache,
                file_io_cache,
            );
            if let Some(token) = initial_token {
                token_file_io = token_file_io.with_initial_token(token);
            }
            return Arc::new(token_file_io).build_file_io().await;
        }

        let mut builder = FileIO::from_path(path)?.with_props(options.to_map());
        builder = builder.with_cache_context(file_io_cache);
        if let Some(local_cache) = local_cache {
            builder = builder.with_local_cache(local_cache);
        }
        builder.build()
    }

    /// Load the authoritative latest snapshot, scoped to this table's branch.
    pub(crate) async fn load_snapshot(
        &self,
        branch: &str,
    ) -> Result<Option<crate::spec::Snapshot>> {
        let identifier = self.identifier.with_branch(branch)?;
        match self.api.load_snapshot(&identifier).await {
            Ok(snapshot) => Ok(snapshot.map(|snapshot| snapshot.snapshot)),
            Err(Error::RestApi {
                source: RestError::NoSuchResource { resource_type, .. },
            }) if resource_type.as_deref() == Some("SNAPSHOT") => Ok(None),
            Err(error) => Err(map_rest_error_for_table(error, &identifier)),
        }
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

fn response_identifier(
    requested: &Identifier,
    response: &crate::api::GetTableResponse,
) -> Result<Identifier> {
    let database = response
        .database
        .as_deref()
        .unwrap_or_else(|| requested.database());
    let name = response.name.as_deref().ok_or_else(|| Error::DataInvalid {
        message: format!("Table response for database '{database}' missing name"),
        source: None,
    })?;
    let identifier = Identifier::new(database, name);
    identifier.validate()?;
    // A REST catalog may return the canonical case for an otherwise matching identifier.
    if !identifier
        .database()
        .eq_ignore_ascii_case(requested.database())
        || !identifier.object().eq_ignore_ascii_case(requested.object())
    {
        return Err(Error::DataInvalid {
            message: format!(
                "Table response identifier '{}' does not match requested identifier '{}'",
                identifier.full_name(),
                requested.full_name()
            ),
            source: None,
        });
    }
    Ok(identifier)
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

    #[test]
    fn response_identifier_accepts_canonicalized_ascii_case() {
        let requested = Identifier::new("Default", "MixedTable");
        let response: GetTableResponse =
            serde_json::from_str(r#"{"database":"default","name":"mixedtable"}"#).unwrap();

        let actual = response_identifier(&requested, &response).unwrap();

        assert_eq!(actual, Identifier::new("default", "mixedtable"));
    }

    #[test]
    fn response_identifier_rejects_a_different_table() {
        let requested = Identifier::new("default", "requested");
        let response: GetTableResponse =
            serde_json::from_str(r#"{"database":"default","name":"different"}"#).unwrap();

        let error = response_identifier(&requested, &response).unwrap_err();

        assert!(matches!(
            error,
            Error::DataInvalid { message, .. }
                if message.contains("does not match requested identifier")
        ));
    }

    #[tokio::test]
    async fn dependency_reads_preserve_the_outermost_identifier_and_catalog_options() {
        let table = crate::table::rest_query_auth_table().await;
        let mut env = table.rest_env().unwrap().clone();
        env.identifier = Identifier::new("db 空格", "root$branch_dev");
        let mut options = env.api.options().clone();
        options.set("prefix", "configured-prefix");
        options.set("header.X-Custom", "kept");
        env.api = Arc::new(RESTApi::new(options, false).await.unwrap());
        // The API has the merged server configuration, while env.options can
        // still be the original catalog options.
        let dependency = env.for_dependency_reads().await.unwrap();
        let key = "header.X-Paimon-Read-Via";
        assert!(env.api.options().get(key).is_none());
        assert!(!Arc::ptr_eq(&env.api, &dependency.api));
        assert_eq!(
            dependency.api.options().get("prefix").unwrap(),
            "configured-prefix"
        );
        assert_eq!(
            dependency.api.options().get("header.X-Custom").unwrap(),
            "kept"
        );
        let decoded =
            crate::api::RESTUtil::decode_string(dependency.api.options().get(key).unwrap());
        assert_eq!(
            decoded,
            r#"{"database":"db 空格","object":"root$branch_dev"}"#
        );
        let mut nested = dependency.clone();
        nested.identifier = Identifier::new("other", "middle");
        let nested = nested.for_dependency_reads().await.unwrap();
        assert!(Arc::ptr_eq(&dependency.api, &nested.api));
        assert_eq!(
            nested.api.options().get(key),
            dependency.api.options().get(key)
        );
    }

    #[tokio::test]
    async fn dependency_reads_preserve_an_explicit_read_via() {
        let table = crate::table::rest_query_auth_table().await;
        let mut env = table.rest_env().unwrap().clone();
        let mut options = env.api.options().clone();
        options.set(
            "header.X-Paimon-Read-Via",
            "an-already-encoded-outermost-table",
        );
        env.api = Arc::new(RESTApi::new(options, false).await.unwrap());
        let dependency = env.for_dependency_reads().await.unwrap();
        assert!(Arc::ptr_eq(&env.api, &dependency.api));
        assert_eq!(
            dependency
                .api
                .options()
                .get("header.X-Paimon-Read-Via")
                .unwrap(),
            "an-already-encoded-outermost-table"
        );
    }

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
        let file_io_cache = FileIOCacheContext::from_props(options.to_map()).unwrap();

        let rest_env = RESTEnv::new(
            Identifier::new("database", "table"),
            "uuid".to_string(),
            api,
            options,
            false,
            local_cache,
            file_io_cache,
        );

        assert!(rest_env.has_local_cache());
        assert!(rest_env.clone().has_local_cache());
    }

    #[tokio::test]
    async fn test_rest_table_loads_share_catalog_metadata_cache() {
        let mut options = Options::new();
        options.set(CatalogOptions::URI, "http://localhost:1");
        options.set(CatalogOptions::WAREHOUSE, "test-warehouse");
        options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
        options.set(CatalogOptions::TOKEN, "test-token");
        let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
        let file_io_cache = FileIOCacheContext::from_props(options.to_map()).unwrap();
        let identifier = Identifier::new("database", "table");

        let first = RESTEnv::build_file_io(
            &identifier,
            "memory:/table",
            api.clone(),
            &options,
            false,
            None,
            file_io_cache.clone(),
            None,
        )
        .await
        .unwrap();
        let second = RESTEnv::build_file_io(
            &identifier,
            "memory:/table",
            api,
            &options,
            false,
            None,
            file_io_cache,
            None,
        )
        .await
        .unwrap();

        assert!(Arc::ptr_eq(
            &first.file_format_metadata_cache(),
            &second.file_format_metadata_cache()
        ));
        assert!(Arc::ptr_eq(
            &first.blob_index_cache(),
            &second.blob_index_cache()
        ));
    }
}

#[cfg(test)]
#[path = "rest_env_file_io_tests.rs"]
mod file_io_tests;
