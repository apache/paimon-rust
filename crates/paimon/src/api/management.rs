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

//! REST management API model: permission assignments and data policies.
//!
//! Mirrors Java `org.apache.paimon.management`. The three policy types deserialize through
//! their constructors, as Java does with `@JsonCreator`, so they cannot hold a value a client
//! could not have sent. The permission types follow Java the other way: its `@JsonCreator`
//! there is the non-validating constructor, so `Deserialize` is derived and the send paths run
//! `canonicalized` or `validate` instead.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::api::rest_error::RestError;
use crate::{Error, Result};

/// Client-side validation failures map to this: a real server answers 400 for the same input.
pub(crate) fn bad_request(message: impl Into<String>) -> Error {
    Error::RestApi {
        source: RestError::BadRequest {
            message: message.into(),
        },
    }
}

/// Java's `String.trim()` strips only characters `<= U+0020`, so a value is blank exactly when
/// every character is one of those; `str::trim` would also strip the rest of Unicode whitespace.
pub(crate) fn is_blank(value: &str) -> bool {
    value.chars().all(|ch| ch <= ' ')
}

/// Java's `String.length()` counts UTF-16 code units, and the wire limits are written against it.
pub(crate) fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn blank_to_none(value: Option<String>) -> Option<String> {
    value.filter(|value| !is_blank(value))
}

/// The kind of object a permission is attached to (Java `ResourceType`).
///
/// `CATALOG_ALL` / `DATABASE_ALL` scope over a catalog's or database's descendants, now and
/// later; every other type names one exact object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Catalog,
    CatalogAll,
    Database,
    DatabaseAll,
    Table,
    Column,
    View,
    Function,
}

impl ResourceType {
    pub const VALUES: [ResourceType; 8] = [
        ResourceType::Catalog,
        ResourceType::CatalogAll,
        ResourceType::Database,
        ResourceType::DatabaseAll,
        ResourceType::Table,
        ResourceType::Column,
        ResourceType::View,
        ResourceType::Function,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceType::Catalog => "CATALOG",
            ResourceType::CatalogAll => "CATALOG_ALL",
            ResourceType::Database => "DATABASE",
            ResourceType::DatabaseAll => "DATABASE_ALL",
            ResourceType::Table => "TABLE",
            ResourceType::Column => "COLUMN",
            ResourceType::View => "VIEW",
            ResourceType::Function => "FUNCTION",
        }
    }
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ResourceType {
    type Err = Error;

    /// Case-insensitive, like Java `ResourceType.fromString`.
    fn from_str(value: &str) -> Result<Self> {
        let upper = value.to_uppercase();
        Self::VALUES
            .into_iter()
            .find(|resource_type| resource_type.as_str() == upper)
            .ok_or_else(|| bad_request(format!("Unknown resource type '{value}'.")))
    }
}

impl Serialize for ResourceType {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ResourceType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// A permission target: a resource type and the locators it needs (Java `PermissionResource`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionResource {
    #[serde(rename = "type")]
    resource_type: ResourceType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    function: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    view: Option<String>,
}

impl PermissionResource {
    pub fn catalog() -> Self {
        Self::unchecked(ResourceType::Catalog, None, None, None, None)
    }

    pub fn catalog_all() -> Self {
        Self::unchecked(ResourceType::CatalogAll, None, None, None, None)
    }

    pub fn database(database: impl Into<String>) -> Self {
        Self::unchecked(
            ResourceType::Database,
            Some(database.into()),
            None,
            None,
            None,
        )
    }

    pub fn database_all(database: impl Into<String>) -> Self {
        Self::unchecked(
            ResourceType::DatabaseAll,
            Some(database.into()),
            None,
            None,
            None,
        )
    }

    pub fn table(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self::unchecked(
            ResourceType::Table,
            Some(database.into()),
            Some(table.into()),
            None,
            None,
        )
    }

    /// The columns of one table; the column range travels in the assignment.
    pub fn column(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self::unchecked(
            ResourceType::Column,
            Some(database.into()),
            Some(table.into()),
            None,
            None,
        )
    }

    pub fn function(database: impl Into<String>, function: impl Into<String>) -> Self {
        Self::unchecked(
            ResourceType::Function,
            Some(database.into()),
            None,
            Some(function.into()),
            None,
        )
    }

    pub fn view(database: impl Into<String>, view: impl Into<String>) -> Self {
        Self::unchecked(
            ResourceType::View,
            Some(database.into()),
            None,
            None,
            Some(view.into()),
        )
    }

    pub fn validate_policy_attachment(&self) -> Result<()> {
        if self.resource_type != ResourceType::Table {
            return Err(bad_request(
                "Policies can currently be attached only to TABLE resources.",
            ));
        }
        Ok(())
    }

    pub fn new(
        resource_type: ResourceType,
        database: Option<&str>,
        table: Option<&str>,
        function: Option<&str>,
        view: Option<&str>,
    ) -> Result<Self> {
        Self::unchecked(
            resource_type,
            database.map(str::to_string),
            table.map(str::to_string),
            function.map(str::to_string),
            view.map(str::to_string),
        )
        .canonicalized()
    }

    /// Blank locators count as absent (Java `blankToNull`); what remains must be exactly the
    /// locators the type needs.
    pub fn canonicalized(mut self) -> Result<Self> {
        self.database = blank_to_none(self.database);
        self.table = blank_to_none(self.table);
        self.function = blank_to_none(self.function);
        self.view = blank_to_none(self.view);
        self.check_locators()?;
        Ok(self)
    }

    fn check_locators(&self) -> Result<()> {
        let resource_type = self.resource_type;
        let (needs_database, needs_table, needs_function, needs_view) = match resource_type {
            ResourceType::Catalog | ResourceType::CatalogAll => (false, false, false, false),
            ResourceType::Database | ResourceType::DatabaseAll => (true, false, false, false),
            ResourceType::Table | ResourceType::Column => (true, true, false, false),
            ResourceType::Function => (true, false, true, false),
            ResourceType::View => (true, false, false, true),
        };
        for (present, needed, name) in [
            (self.database.is_some(), needs_database, "database"),
            (self.table.is_some(), needs_table, "table"),
            (self.function.is_some(), needs_function, "function"),
            (self.view.is_some(), needs_view, "view"),
        ] {
            if needed && !present {
                return Err(bad_request(format!(
                    "{name} is required for {resource_type} resource."
                )));
            }
            if !needed && present {
                return Err(bad_request(format!(
                    "{resource_type} resource cannot contain {name}."
                )));
            }
        }
        Ok(())
    }

    fn unchecked(
        resource_type: ResourceType,
        database: Option<String>,
        table: Option<String>,
        function: Option<String>,
        view: Option<String>,
    ) -> Self {
        Self {
            resource_type,
            database,
            table,
            function,
            view,
        }
    }

    pub fn resource_type(&self) -> ResourceType {
        self.resource_type
    }

    /// The locator getters carry a `_name` suffix because the plain names belong to the
    /// constructors above; Rust allows only one inherent item per name.
    pub fn database_name(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn table_name(&self) -> Option<&str> {
        self.table.as_deref()
    }

    pub fn function_name(&self) -> Option<&str> {
        self.function.as_deref()
    }

    pub fn view_name(&self) -> Option<&str> {
        self.view.as_deref()
    }
}

/// The built-in access names and their canonical (upper-case) form (Java `PermissionAccess`).
pub struct PermissionAccess;

impl PermissionAccess {
    /// Maximum access-name length, in UTF-16 code units.
    pub const MAX_LENGTH: usize = 32;
    pub const ALL: &'static str = "ALL";
    pub const CREATEDATABASE: &'static str = "CREATEDATABASE";
    pub const DESCRIBE: &'static str = "DESCRIBE";
    pub const ALTER: &'static str = "ALTER";
    pub const DROP: &'static str = "DROP";
    pub const CREATETABLE: &'static str = "CREATETABLE";
    pub const CREATEFUNCTION: &'static str = "CREATEFUNCTION";
    pub const CREATEVIEW: &'static str = "CREATEVIEW";
    pub const LIST: &'static str = "LIST";
    pub const SELECT: &'static str = "SELECT";
    pub const UPDATE: &'static str = "UPDATE";
    pub const GRANT: &'static str = "GRANT";

    pub fn built_ins(resource_type: ResourceType) -> &'static [&'static str] {
        match resource_type {
            ResourceType::Catalog => &[
                Self::ALL,
                Self::ALTER,
                Self::DROP,
                Self::GRANT,
                Self::CREATEDATABASE,
            ],
            ResourceType::CatalogAll => &[
                Self::ALL,
                Self::DESCRIBE,
                Self::ALTER,
                Self::DROP,
                Self::GRANT,
                Self::CREATETABLE,
                Self::CREATEVIEW,
                Self::CREATEFUNCTION,
                Self::LIST,
                Self::SELECT,
                Self::UPDATE,
            ],
            ResourceType::Database => &[
                Self::ALL,
                Self::DESCRIBE,
                Self::ALTER,
                Self::DROP,
                Self::GRANT,
                Self::CREATETABLE,
                Self::CREATEVIEW,
                Self::CREATEFUNCTION,
                Self::LIST,
            ],
            ResourceType::DatabaseAll | ResourceType::Table => &[
                Self::ALL,
                Self::SELECT,
                Self::UPDATE,
                Self::ALTER,
                Self::DROP,
                Self::GRANT,
            ],
            ResourceType::Column => &[Self::SELECT],
            ResourceType::View | ResourceType::Function => &[
                Self::ALL,
                Self::SELECT,
                Self::ALTER,
                Self::DROP,
                Self::GRANT,
            ],
        }
    }

    /// Upper-cases `access`; the length is checked again afterwards because upper-casing can
    /// grow a string ("ß" -> "SS").
    pub fn canonicalize(access: &str) -> Result<String> {
        if is_blank(access) {
            return Err(bad_request("access cannot be empty."));
        }
        if utf16_len(access) > Self::MAX_LENGTH {
            return Err(bad_request(format!(
                "access must contain at most {} characters.",
                Self::MAX_LENGTH
            )));
        }
        let canonical = access.to_uppercase();
        if utf16_len(&canonical) > Self::MAX_LENGTH {
            return Err(bad_request(format!(
                "access must contain at most {} characters after canonicalization.",
                Self::MAX_LENGTH
            )));
        }
        let known = ResourceType::VALUES
            .into_iter()
            .any(|resource_type| Self::built_ins(resource_type).contains(&canonical.as_str()));
        if !known {
            return Err(bad_request(format!("Unknown access '{canonical}'.")));
        }
        Ok(canonical)
    }

    pub fn canonicalize_for(resource_type: ResourceType, access: &str) -> Result<String> {
        let canonical = Self::canonicalize(access)?;
        if !Self::built_ins(resource_type).contains(&canonical.as_str()) {
            return Err(bad_request(format!(
                "Access '{canonical}' is not valid for {resource_type}."
            )));
        }
        Ok(canonical)
    }
}

/// The column range of a `COLUMN` assignment: an allowlist (`columnNames`) or a denylist
/// (`excludedColumnNames`), never both (Java `PermissionColumns`). Names are top-level fields;
/// the allowlist denies columns added later, the denylist allows them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionColumns {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    column_names: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    excluded_column_names: Option<Vec<String>>,
}

impl PermissionColumns {
    pub fn names(column_names: Vec<String>) -> Result<Self> {
        Self::check(&column_names, "columnNames")?;
        Ok(Self {
            column_names: Some(column_names),
            excluded_column_names: None,
        })
    }

    pub fn excluded(excluded_column_names: Vec<String>) -> Result<Self> {
        Self::check(&excluded_column_names, "excludedColumnNames")?;
        Ok(Self {
            column_names: None,
            excluded_column_names: Some(excluded_column_names),
        })
    }

    /// The constructors' rules: exactly one list, non-empty, no blank or duplicate name.
    pub fn validate(&self) -> Result<()> {
        match (&self.column_names, &self.excluded_column_names) {
            (Some(columns), None) => Self::check(columns, "columnNames"),
            (None, Some(columns)) => Self::check(columns, "excludedColumnNames"),
            _ => Err(bad_request(
                "columns must contain exactly one of columnNames or excludedColumnNames.",
            )),
        }
    }

    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    pub fn excluded_column_names(&self) -> Option<&[String]> {
        self.excluded_column_names.as_deref()
    }

    fn check(columns: &[String], field: &str) -> Result<()> {
        if columns.is_empty() {
            return Err(bad_request(format!("{field} cannot be empty.")));
        }
        if columns.iter().any(|column| is_blank(column)) {
            return Err(bad_request(format!(
                "{field} cannot contain an empty column name."
            )));
        }
        let unique: std::collections::HashSet<&String> = columns.iter().collect();
        if unique.len() != columns.len() {
            return Err(bad_request(format!(
                "{field} cannot contain duplicate column names."
            )));
        }
        Ok(())
    }
}

/// One access granted to one principal on one resource (Java `PermissionAssignment`).
///
/// The identity is `(resource, access, principal)`; `expire_time` is an exclusive upper bound
/// on the server clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionAssignment {
    resource: PermissionResource,
    access: String,
    principal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    columns: Option<PermissionColumns>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expire_time: Option<String>,
}

impl PermissionAssignment {
    /// Maximum principal length, in UTF-16 code units.
    pub const MAX_PRINCIPAL_LENGTH: usize = 128;

    pub fn new(
        resource: PermissionResource,
        access: &str,
        principal: &str,
        columns: Option<PermissionColumns>,
        expire_time: Option<&str>,
    ) -> Result<Self> {
        Self {
            resource,
            access: access.to_string(),
            principal: principal.to_string(),
            columns,
            expire_time: expire_time.map(str::to_string),
        }
        .canonicalized()
    }

    pub fn canonicalized(mut self) -> Result<Self> {
        self.access =
            PermissionAccess::canonicalize_for(self.resource.resource_type(), &self.access)?;
        Self::validate_principal(&self.principal)?;
        self.resource = self.resource.canonicalized()?;
        match (
            self.resource.resource_type() == ResourceType::Column,
            self.columns.is_some(),
        ) {
            (true, false) => return Err(bad_request("columns is required for COLUMN resource.")),
            (false, true) => return Err(bad_request("columns is only valid for COLUMN resource.")),
            _ => {}
        }
        if let Some(columns) = &self.columns {
            columns.validate()?;
        }
        if let Some(expire_time) = &self.expire_time {
            validate_expire_time(expire_time)?;
        }
        Ok(self)
    }

    /// Principals are opaque, server-defined strings: only blankness and length are checked.
    pub fn validate_principal(principal: &str) -> Result<()> {
        if is_blank(principal) {
            return Err(bad_request("principal cannot be empty."));
        }
        if utf16_len(principal) > Self::MAX_PRINCIPAL_LENGTH {
            return Err(bad_request(format!(
                "principal must contain at most {} characters.",
                Self::MAX_PRINCIPAL_LENGTH
            )));
        }
        Ok(())
    }

    pub fn resource(&self) -> &PermissionResource {
        &self.resource
    }

    pub fn access(&self) -> &str {
        &self.access
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    pub fn columns(&self) -> Option<&PermissionColumns> {
        self.columns.as_ref()
    }

    pub fn expire_time(&self) -> Option<&str> {
        self.expire_time.as_deref()
    }
}

fn validate_expire_time(expire_time: &str) -> Result<()> {
    // Java parses with `Instant.parse` (ISO_INSTANT, case-insensitive), which takes the `Z`
    // offset and no other, so an offset RFC-3339 allows here the server would still refuse.
    let instant = chrono::DateTime::parse_from_rfc3339(expire_time)
        .ok()
        .filter(|_| expire_time.ends_with(['Z', 'z']))
        .ok_or_else(|| bad_request("expireTime must be an ISO-8601 UTC instant."))?;
    if instant.timestamp_subsec_nanos() % 1_000_000 != 0 {
        return Err(bad_request(
            "expireTime must have at most millisecond precision.",
        ));
    }
    Ok(())
}

pub(crate) fn validate_max_results(max_results: u32) -> Result<()> {
    if max_results == 0 || max_results > ListPermissionsRequest::MAX_PAGE_SIZE {
        return Err(bad_request(format!(
            "maxResults must be between 1 and {}.",
            ListPermissionsRequest::MAX_PAGE_SIZE
        )));
    }
    Ok(())
}

/// Filters for `GET {prefix}/permissions` (Java `ListPermissionsRequest`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPermissionsRequest {
    pub resource: PermissionResource,
    pub principal: Option<String>,
    pub access: Option<String>,
    /// Page size, `1..=MAX_PAGE_SIZE`.
    pub max_results: Option<u32>,
    /// Opaque continuation token from the previous page.
    pub page_token: Option<String>,
}

impl ListPermissionsRequest {
    /// Largest page a server has to honour.
    pub const MAX_PAGE_SIZE: u32 = 1000;

    pub fn new(resource: PermissionResource) -> Self {
        Self {
            resource,
            principal: None,
            access: None,
            max_results: None,
            page_token: None,
        }
    }

    /// The query string as `(name, value)` pairs, in the order Java sends them.
    pub fn query_params(&self) -> Result<Vec<(&'static str, String)>> {
        let resource = self.resource.clone().canonicalized()?;
        let resource_type = resource.resource_type();
        let mut params = vec![("resourceType", resource_type.to_string())];
        for (name, value) in [
            ("database", resource.database_name()),
            ("table", resource.table_name()),
            ("function", resource.function_name()),
            ("view", resource.view_name()),
        ] {
            if let Some(value) = value {
                params.push((name, value.to_string()));
            }
        }
        if let Some(principal) = self.principal.as_deref().filter(|value| !is_blank(value)) {
            PermissionAssignment::validate_principal(principal)?;
            params.push(("principal", principal.to_string()));
        }
        if let Some(access) = self.access.as_deref().filter(|value| !is_blank(value)) {
            params.push((
                "access",
                PermissionAccess::canonicalize_for(resource_type, access)?,
            ));
        }
        if let Some(max_results) = self.max_results {
            validate_max_results(max_results)?;
            params.push(("maxResults", max_results.to_string()));
        }
        if let Some(page_token) = self.page_token.as_deref().filter(|value| !value.is_empty()) {
            params.push(("pageToken", page_token.to_string()));
        }
        Ok(params)
    }
}

/// The kind of a data policy (Java `PolicyType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyType {
    RowFilter,
    ColumnMasking,
}

impl PolicyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyType::RowFilter => "ROW_FILTER",
            PolicyType::ColumnMasking => "COLUMN_MASKING",
        }
    }
}

impl fmt::Display for PolicyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PolicyType {
    type Err = Error;

    /// Case-insensitive, like Java `PolicyType.fromString`.
    fn from_str(value: &str) -> Result<Self> {
        [PolicyType::RowFilter, PolicyType::ColumnMasking]
            .into_iter()
            .find(|policy_type| policy_type.as_str() == value.to_uppercase())
            .ok_or_else(|| bad_request(format!("Unknown policy type '{value}'.")))
    }
}

impl Serialize for PolicyType {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PolicyType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Largest serialized predicate or transform a policy may carry, in UTF-8 bytes.
const MAX_POLICY_PAYLOAD_BYTES: usize = 60 * 1024;

fn check_payload(value: &str, field: &str) -> Result<()> {
    if is_blank(value) {
        return Err(bad_request(format!("{field} cannot be empty.")));
    }
    if value.len() > MAX_POLICY_PAYLOAD_BYTES {
        return Err(bad_request(format!(
            "{field} must not exceed {MAX_POLICY_PAYLOAD_BYTES} UTF-8 bytes."
        )));
    }
    Ok(())
}

/// A serialized Paimon `Predicate` applied to every scan of the table (Java `RowFilter`).
/// Same JSON as one entry of `AuthTableQueryResponse::filter`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RowFilter {
    predicate: String,
}

impl RowFilter {
    pub const MAX_PREDICATE_BYTES: usize = MAX_POLICY_PAYLOAD_BYTES;

    pub fn new(predicate: impl Into<String>) -> Result<Self> {
        let predicate = predicate.into();
        check_payload(&predicate, "predicate")?;
        Ok(Self { predicate })
    }

    pub fn predicate(&self) -> &str {
        &self.predicate
    }
}

/// A serialized Paimon `Transform` whose result replaces `on_column` (Java `ColumnMask`).
/// Same JSON as one value of `AuthTableQueryResponse::column_masking`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMask {
    on_column: String,
    transform: String,
}

impl ColumnMask {
    pub const MAX_TRANSFORM_BYTES: usize = MAX_POLICY_PAYLOAD_BYTES;

    pub fn new(on_column: impl Into<String>, transform: impl Into<String>) -> Result<Self> {
        let on_column = on_column.into();
        if is_blank(&on_column) {
            return Err(bad_request("onColumn cannot be empty."));
        }
        let transform = transform.into();
        check_payload(&transform, "transform")?;
        Ok(Self {
            on_column,
            transform,
        })
    }

    pub fn on_column(&self) -> &str {
        &self.on_column
    }

    pub fn transform(&self) -> &str {
        &self.transform
    }
}

/// One row filter or one column mask attached to a table for one principal
/// (Java `DataPolicy`). A row filter is identified by `(table, principal)`, a column mask by
/// `(table, principal, on_column)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataPolicy {
    resource: PermissionResource,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_filter: Option<RowFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    column_mask: Option<ColumnMask>,
    principal: String,
}

impl DataPolicy {
    /// Java `DataPolicy.rowFilter`. The `new_` prefix is forced: `row_filter` is the getter.
    pub fn new_row_filter(
        resource: PermissionResource,
        row_filter: RowFilter,
        principal: &str,
    ) -> Result<Self> {
        Self::new(resource, Some(row_filter), None, principal)
    }

    /// Java `DataPolicy.columnMask`.
    pub fn new_column_mask(
        resource: PermissionResource,
        column_mask: ColumnMask,
        principal: &str,
    ) -> Result<Self> {
        Self::new(resource, None, Some(column_mask), principal)
    }

    pub fn new(
        resource: PermissionResource,
        row_filter: Option<RowFilter>,
        column_mask: Option<ColumnMask>,
        principal: &str,
    ) -> Result<Self> {
        Self {
            resource,
            row_filter,
            column_mask,
            principal: principal.to_string(),
        }
        .canonicalized()
    }

    /// A policy corrects nothing of its own; only its resource can need canonicalizing.
    fn canonicalized(mut self) -> Result<Self> {
        self.resource.validate_policy_attachment()?;
        self.resource = self.resource.canonicalized()?;
        PermissionAssignment::validate_principal(&self.principal)?;
        if self.row_filter.is_none() == self.column_mask.is_none() {
            return Err(bad_request(
                "A policy must contain exactly one of rowFilter and columnMask.",
            ));
        }
        Ok(self)
    }

    pub fn policy_type(&self) -> PolicyType {
        if self.row_filter.is_some() {
            PolicyType::RowFilter
        } else {
            PolicyType::ColumnMasking
        }
    }

    pub fn resource(&self) -> &PermissionResource {
        &self.resource
    }

    pub fn row_filter(&self) -> Option<&RowFilter> {
        self.row_filter.as_ref()
    }

    pub fn column_mask(&self) -> Option<&ColumnMask> {
        self.column_mask.as_ref()
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }
}

// The three policy types deserialize through their constructors, so a value arriving from the
// wire has passed the same checks as one built locally. Java gets this from `@JsonCreator`.

impl<'de> Deserialize<'de> for RowFilter {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Repr {
            predicate: String,
        }
        let repr = Repr::deserialize(deserializer)?;
        Self::new(repr.predicate).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for ColumnMask {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Repr {
            on_column: String,
            transform: String,
        }
        let repr = Repr::deserialize(deserializer)?;
        Self::new(repr.on_column, repr.transform).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for DataPolicy {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Repr {
            resource: PermissionResource,
            #[serde(default)]
            row_filter: Option<RowFilter>,
            #[serde(default)]
            column_mask: Option<ColumnMask>,
            principal: String,
        }
        let repr = Repr::deserialize(deserializer)?;
        Self::new(
            repr.resource,
            repr.row_filter,
            repr.column_mask,
            &repr.principal,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Filters for `GET .../tables/{table}/policies` (Java `ListPoliciesRequest`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPoliciesRequest {
    /// The table the policies are attached to; must be a `TABLE` resource.
    pub resource: PermissionResource,
    pub policy_type: Option<PolicyType>,
    pub principal: Option<String>,
    /// Only masks on this column; requires `policy_type == Some(PolicyType::ColumnMasking)`.
    pub column: Option<String>,
    pub max_results: Option<u32>,
    pub page_token: Option<String>,
}

impl ListPoliciesRequest {
    pub fn new(resource: PermissionResource) -> Self {
        Self {
            resource,
            policy_type: None,
            principal: None,
            column: None,
            max_results: None,
            page_token: None,
        }
    }

    /// The validated query string as `(name, value)` pairs, in the order Java sends them.
    pub fn query_params(&self) -> Result<Vec<(&'static str, String)>> {
        self.resource.validate_policy_attachment()?;
        let mut params = Vec::new();
        if let Some(policy_type) = self.policy_type {
            params.push(("type", policy_type.to_string()));
        }
        if let Some(principal) = self.principal.as_deref().filter(|value| !is_blank(value)) {
            PermissionAssignment::validate_principal(principal)?;
            params.push(("principal", principal.to_string()));
        }
        if let Some(column) = self.column.as_deref().filter(|value| !is_blank(value)) {
            if self.policy_type != Some(PolicyType::ColumnMasking) {
                return Err(bad_request("column filter requires type COLUMN_MASKING."));
            }
            params.push(("column", column.to_string()));
        }
        if let Some(max_results) = self.max_results {
            validate_max_results(max_results)?;
            params.push(("maxResults", max_results.to_string()));
        }
        if let Some(page_token) = self.page_token.as_deref().filter(|value| !value.is_empty()) {
            params.push(("pageToken", page_token.to_string()));
        }
        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_resource() -> PermissionResource {
        PermissionResource::table("sales", "orders")
    }

    #[test]
    fn test_resource_type_wire_names_are_upper_snake_and_read_case_insensitively() {
        assert_eq!(
            serde_json::to_string(&ResourceType::CatalogAll).unwrap(),
            r#""CATALOG_ALL""#
        );
        assert_eq!(
            serde_json::from_str::<ResourceType>(r#""database_all""#).unwrap(),
            ResourceType::DatabaseAll
        );
        assert_eq!("view".parse::<ResourceType>().unwrap(), ResourceType::View);
        assert!("SCHEMA".parse::<ResourceType>().is_err());
        assert_eq!(ResourceType::Function.to_string(), "FUNCTION");
    }

    #[test]
    fn test_resource_serialization_omits_absent_locators() {
        assert_eq!(
            serde_json::to_string(&PermissionResource::catalog()).unwrap(),
            r#"{"type":"CATALOG"}"#
        );
        assert_eq!(
            serde_json::to_string(&PermissionResource::catalog_all()).unwrap(),
            r#"{"type":"CATALOG_ALL"}"#
        );
        assert_eq!(
            serde_json::to_string(&PermissionResource::database_all("sales")).unwrap(),
            r#"{"type":"DATABASE_ALL","database":"sales"}"#
        );
        assert_eq!(
            serde_json::to_string(&table_resource()).unwrap(),
            r#"{"type":"TABLE","database":"sales","table":"orders"}"#
        );
        assert_eq!(
            serde_json::to_string(&PermissionResource::function("sales", "calculate_tax")).unwrap(),
            r#"{"type":"FUNCTION","database":"sales","function":"calculate_tax"}"#
        );
        assert_eq!(
            serde_json::to_string(&PermissionResource::view("sales", "daily_orders")).unwrap(),
            r#"{"type":"VIEW","database":"sales","view":"daily_orders"}"#
        );
        let parsed: PermissionResource = serde_json::from_str(
            r#"{"type":"table","database":"sales","table":"orders","extra":1}"#,
        )
        .unwrap();
        assert_eq!(parsed, table_resource());
    }

    #[test]
    fn test_resource_new_canonicalizes_blank_locators_and_enforces_the_type_rules() {
        assert_eq!(
            PermissionResource::new(ResourceType::Catalog, Some(""), Some(" "), None, None)
                .unwrap(),
            PermissionResource::catalog()
        );
        assert_eq!(
            PermissionResource::new(
                ResourceType::Function,
                Some("sales"),
                None,
                Some("calculate_tax"),
                None
            )
            .unwrap(),
            PermissionResource::function("sales", "calculate_tax")
        );
        let message = |result: Result<PermissionResource>| result.unwrap_err().to_string();
        assert!(message(PermissionResource::new(
            ResourceType::Catalog,
            Some("sales"),
            None,
            None,
            None
        ))
        .contains("CATALOG resource cannot contain database"));
        assert!(message(PermissionResource::new(
            ResourceType::Database,
            None,
            None,
            None,
            None
        ))
        .contains("database is required for DATABASE"));
        assert!(message(PermissionResource::new(
            ResourceType::Table,
            Some("sales"),
            None,
            None,
            None
        ))
        .contains("table is required for TABLE"));
        assert!(message(PermissionResource::new(
            ResourceType::Column,
            Some("sales"),
            Some("orders"),
            Some("f"),
            None
        ))
        .contains("COLUMN resource cannot contain function"));
        assert!(message(PermissionResource::new(
            ResourceType::Function,
            Some("sales"),
            None,
            None,
            Some("v")
        ))
        .contains("function is required for FUNCTION"));
        assert!(message(PermissionResource::new(
            ResourceType::View,
            Some("sales"),
            Some("orders"),
            None,
            Some("v")
        ))
        .contains("VIEW resource cannot contain table"));
    }

    #[test]
    fn test_access_built_ins_match_java() {
        let sorted = |resource_type| {
            let mut values = PermissionAccess::built_ins(resource_type).to_vec();
            values.sort_unstable();
            values
        };
        assert_eq!(
            sorted(ResourceType::Catalog),
            ["ALL", "ALTER", "CREATEDATABASE", "DROP", "GRANT"]
        );
        assert_eq!(
            sorted(ResourceType::CatalogAll),
            [
                "ALL",
                "ALTER",
                "CREATEFUNCTION",
                "CREATETABLE",
                "CREATEVIEW",
                "DESCRIBE",
                "DROP",
                "GRANT",
                "LIST",
                "SELECT",
                "UPDATE"
            ]
        );
        assert_eq!(
            sorted(ResourceType::Database),
            [
                "ALL",
                "ALTER",
                "CREATEFUNCTION",
                "CREATETABLE",
                "CREATEVIEW",
                "DESCRIBE",
                "DROP",
                "GRANT",
                "LIST"
            ]
        );
        assert_eq!(
            sorted(ResourceType::DatabaseAll),
            ["ALL", "ALTER", "DROP", "GRANT", "SELECT", "UPDATE"]
        );
        assert_eq!(
            sorted(ResourceType::Table),
            ["ALL", "ALTER", "DROP", "GRANT", "SELECT", "UPDATE"]
        );
        assert_eq!(
            sorted(ResourceType::View),
            ["ALL", "ALTER", "DROP", "GRANT", "SELECT"]
        );
        assert_eq!(
            sorted(ResourceType::Function),
            ["ALL", "ALTER", "DROP", "GRANT", "SELECT"]
        );
        assert_eq!(sorted(ResourceType::Column), ["SELECT"]);
    }

    #[test]
    fn test_access_canonicalization() {
        assert_eq!(
            PermissionAccess::canonicalize("createdatabase").unwrap(),
            "CREATEDATABASE"
        );
        assert_eq!(
            PermissionAccess::canonicalize_for(ResourceType::Database, "createview").unwrap(),
            "CREATEVIEW"
        );
        let message = |result: Result<String>| result.unwrap_err().to_string();
        assert!(message(PermissionAccess::canonicalize(" ")).contains("access cannot be empty"));
        assert!(message(PermissionAccess::canonicalize_for(
            ResourceType::Catalog,
            "SELECT"
        ))
        .contains("Access 'SELECT' is not valid for CATALOG"));
        assert!(message(PermissionAccess::canonicalize_for(
            ResourceType::CatalogAll,
            "CREATEDATABASE"
        ))
        .contains("not valid for CATALOG_ALL"));
        assert!(message(PermissionAccess::canonicalize_for(
            ResourceType::DatabaseAll,
            "LIST"
        ))
        .contains("not valid for DATABASE_ALL"));
        for access in [
            "USE_CATALOG",
            "CREATE_DATABASE",
            "USE_DATABASE",
            "CREATE_TABLE",
            "CREATE_VIEW",
            "CREATE_FUNCTION",
            "INSERT",
            "DELETE",
            "EXECUTE",
            "MANAGE_PERMISSIONS",
            "vendor.example/read_sensitive",
        ] {
            assert!(
                message(PermissionAccess::canonicalize(access)).contains("Unknown access"),
                "{access}"
            );
        }
        assert!(message(PermissionAccess::canonicalize(&"A".repeat(33))).contains("32"));
        // 18 code units before upper-casing, 34 after.
        assert!(message(PermissionAccess::canonicalize(&format!(
            "a/{}",
            "ß".repeat(16)
        )))
        .contains("after canonicalization"));
    }

    const ASSIGNMENT_JSON: &str = r#"{"resource":{"type":"TABLE","database":"sales","table":"orders"},"access":"SELECT","principal":"analyst","expireTime":"2027-01-01T00:00:00Z"}"#;
    const COLUMN_ASSIGNMENT_JSON: &str = r#"{"resource":{"type":"COLUMN","database":"sales","table":"orders"},"access":"SELECT","principal":"analyst","columns":{"columnNames":["id","region"]}}"#;

    #[test]
    fn test_assignment_round_trips_java_wire_json() {
        let assignment: PermissionAssignment = serde_json::from_str(ASSIGNMENT_JSON).unwrap();
        assert_eq!(assignment.resource(), &table_resource());
        assert_eq!(assignment.access(), "SELECT");
        assert_eq!(assignment.principal(), "analyst");
        assert_eq!(assignment.expire_time(), Some("2027-01-01T00:00:00Z"));
        assert_eq!(assignment.columns(), None);
        assert_eq!(serde_json::to_string(&assignment).unwrap(), ASSIGNMENT_JSON);
        let lower: PermissionAssignment =
            serde_json::from_str(&ASSIGNMENT_JSON.replace("\"TABLE\"", "\"table\"")).unwrap();
        assert_eq!(lower, assignment);
        let built = PermissionAssignment::new(
            table_resource(),
            "select",
            "analyst",
            None,
            Some("2027-01-01T00:00:00Z"),
        )
        .unwrap();
        assert_eq!(built, assignment);
    }

    #[test]
    fn test_column_assignment_round_trips_java_wire_json() {
        let assignment: PermissionAssignment =
            serde_json::from_str(COLUMN_ASSIGNMENT_JSON).unwrap();
        assert_eq!(assignment.resource().resource_type(), ResourceType::Column);
        let columns = assignment.columns().unwrap();
        assert_eq!(
            columns.column_names(),
            Some(&["id".to_string(), "region".to_string()][..])
        );
        assert_eq!(columns.excluded_column_names(), None);
        assert_eq!(
            serde_json::to_string(&assignment).unwrap(),
            COLUMN_ASSIGNMENT_JSON
        );
        let excluded = PermissionColumns::excluded(vec!["email".to_string()]).unwrap();
        assert_eq!(
            serde_json::to_string(&excluded).unwrap(),
            r#"{"excludedColumnNames":["email"]}"#
        );
    }

    #[test]
    fn test_responses_are_not_validated_but_requests_are() {
        let precise =
            ASSIGNMENT_JSON.replace("2027-01-01T00:00:00Z", "2027-01-01T00:00:00.123456Z");
        let listed: PermissionAssignment = serde_json::from_str(&precise).unwrap();
        assert_eq!(listed.expire_time(), Some("2027-01-01T00:00:00.123456Z"));
        let message = |expire_time: &str| {
            PermissionAssignment::new(
                table_resource(),
                "SELECT",
                "analyst",
                None,
                Some(expire_time),
            )
            .unwrap_err()
            .to_string()
        };
        assert!(message("2027-01-01T00:00:00.123456Z").contains("millisecond"));
        assert!(message("2027-01-01T00:00:00.000001Z").contains("millisecond"));
        assert!(message("tomorrow").contains("ISO-8601"));
        assert!(message("2027-01-01T00:00:00+08:00").contains("ISO-8601"));
        for expire_time in ["2027-01-01T00:00:00Z", "2027-01-01T00:00:00.123Z"] {
            assert!(
                PermissionAssignment::new(
                    table_resource(),
                    "SELECT",
                    "analyst",
                    None,
                    Some(expire_time),
                )
                .is_ok(),
                "{expire_time}"
            );
        }
    }

    #[test]
    fn test_assignment_validation_matches_java() {
        let included =
            PermissionColumns::names(vec!["id".to_string(), "region".to_string()]).unwrap();
        let column_resource = PermissionResource::column("sales", "orders");
        assert_eq!(
            PermissionAssignment::new(
                column_resource.clone(),
                "select",
                "analyst",
                Some(included.clone()),
                None
            )
            .unwrap()
            .columns(),
            Some(&included)
        );
        assert_eq!(
            PermissionAssignment::new(
                PermissionResource::catalog(),
                "createdatabase",
                "analyst",
                None,
                None
            )
            .unwrap()
            .access(),
            "CREATEDATABASE"
        );
        let rejects = |resource: PermissionResource,
                       access: &str,
                       principal: &str,
                       columns: Option<PermissionColumns>,
                       needle: &str| {
            let message = PermissionAssignment::new(resource, access, principal, columns, None)
                .unwrap_err()
                .to_string();
            assert!(
                message.contains(needle),
                "{message:?} should mention {needle:?}"
            );
        };
        rejects(
            PermissionResource::catalog(),
            "SELECT",
            "analyst",
            None,
            "not valid for CATALOG",
        );
        rejects(
            PermissionResource::database("sales"),
            "SELECT",
            "analyst",
            None,
            "not valid for DATABASE",
        );
        rejects(
            table_resource(),
            "CREATEVIEW",
            "analyst",
            None,
            "not valid for TABLE",
        );
        rejects(
            PermissionResource::function("sales", "calculate_tax"),
            "UPDATE",
            "analyst",
            None,
            "not valid for FUNCTION",
        );
        rejects(
            column_resource.clone(),
            "UPDATE",
            "analyst",
            Some(included.clone()),
            "not valid for COLUMN",
        );
        rejects(
            column_resource,
            "SELECT",
            "analyst",
            None,
            "columns is required",
        );
        rejects(
            table_resource(),
            "SELECT",
            "analyst",
            Some(included),
            "only valid for COLUMN",
        );
        rejects(
            table_resource(),
            "SELECT",
            " ",
            None,
            "principal cannot be empty",
        );
        rejects(table_resource(), "SELECT", &"p".repeat(129), None, "128");
        assert!(PermissionAssignment::validate_principal(&"p".repeat(128)).is_ok());
    }

    #[test]
    fn test_column_range_rules() {
        let message = |result: Result<PermissionColumns>| result.unwrap_err().to_string();
        assert!(message(PermissionColumns::names(vec![])).contains("columnNames cannot be empty"));
        assert!(message(PermissionColumns::excluded(vec![]))
            .contains("excludedColumnNames cannot be empty"));
        assert!(message(PermissionColumns::names(vec![
            "id".to_string(),
            "id".to_string()
        ]))
        .contains("duplicate"));
        assert!(message(PermissionColumns::excluded(vec![" ".to_string()]))
            .contains("empty column name"));
    }

    #[test]
    fn test_assignment_rejects_a_column_range_no_constructor_could_have_built() {
        let neither = serde_json::from_str::<PermissionColumns>(r#"{}"#).unwrap();
        let both = serde_json::from_str::<PermissionColumns>(
            r#"{"columnNames":["id"],"excludedColumnNames":["email"]}"#,
        )
        .unwrap();
        let assign = |columns: PermissionColumns| {
            PermissionAssignment::new(
                PermissionResource::column("sales", "orders"),
                "SELECT",
                "analyst",
                Some(columns),
                None,
            )
        };
        for columns in [neither, both] {
            let message = assign(columns).unwrap_err().to_string();
            assert!(message.contains("exactly one"), "{message:?}");
        }
        assert!(assign(PermissionColumns::names(vec!["id".to_string()]).unwrap()).is_ok());
    }

    #[test]
    fn test_send_paths_reject_values_no_constructor_could_have_built() {
        let blank_table = PermissionResource::table("sales", "");
        let message = |error: Error| error.to_string();
        assert!(message(blank_table.clone().canonicalized().unwrap_err())
            .contains("table is required for TABLE"));
        assert!(message(
            PermissionResource::database(" ")
                .canonicalized()
                .unwrap_err()
        )
        .contains("database is required for DATABASE"));
        assert!(table_resource().canonicalized().is_ok());
        assert!(ListPermissionsRequest::new(blank_table.clone())
            .query_params()
            .unwrap_err()
            .to_string()
            .contains("table is required for TABLE"));
        assert!(
            PermissionAssignment::new(blank_table, "SELECT", "analyst", None, None)
                .unwrap_err()
                .to_string()
                .contains("table is required for TABLE")
        );
        let empty = serde_json::from_str::<PermissionColumns>(r#"{"columnNames":[]}"#).unwrap();
        assert!(message(empty.validate().unwrap_err()).contains("columnNames cannot be empty"));
        let rejected = PermissionAssignment::new(
            PermissionResource::column("sales", "orders"),
            "SELECT",
            "analyst",
            Some(empty),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            rejected.contains("columnNames cannot be empty"),
            "{rejected:?}"
        );
        for json in [
            r#"{"columnNames":[" "]}"#,
            r#"{"excludedColumnNames":["id","id"]}"#,
        ] {
            let columns = serde_json::from_str::<PermissionColumns>(json).unwrap();
            assert!(columns.validate().is_err(), "{json}");
        }
    }

    #[test]
    fn test_send_paths_canonicalize_values_no_constructor_could_have_built() {
        let blank_view: PermissionResource = serde_json::from_str(
            r#"{"type":"TABLE","database":"sales","table":"orders","view":""}"#,
        )
        .unwrap();
        let assignment =
            PermissionAssignment::new(blank_view.clone(), "select", "analyst", None, None).unwrap();
        assert_eq!(
            serde_json::to_string(&assignment).unwrap(),
            r#"{"resource":{"type":"TABLE","database":"sales","table":"orders"},"access":"SELECT","principal":"analyst"}"#
        );
        assert_eq!(
            ListPermissionsRequest::new(blank_view)
                .query_params()
                .unwrap(),
            vec![
                ("resourceType", "TABLE".to_string()),
                ("database", "sales".to_string()),
                ("table", "orders".to_string()),
            ]
        );
    }

    #[test]
    fn test_list_permissions_request_query_params() {
        let mut request = ListPermissionsRequest::new(PermissionResource::database("sales"));
        request.access = Some("createview".to_string());
        request.principal = Some("analyst".to_string());
        request.max_results = Some(25);
        request.page_token = Some("start".to_string());
        assert_eq!(
            request.query_params().unwrap(),
            vec![
                ("resourceType", "DATABASE".to_string()),
                ("database", "sales".to_string()),
                ("principal", "analyst".to_string()),
                ("access", "CREATEVIEW".to_string()),
                ("maxResults", "25".to_string()),
                ("pageToken", "start".to_string()),
            ]
        );
        let mut request =
            ListPermissionsRequest::new(PermissionResource::function("sales", "calculate_tax"));
        request.principal = Some("  ".to_string());
        request.page_token = Some(" \t".to_string());
        assert_eq!(
            request.query_params().unwrap(),
            vec![
                ("resourceType", "FUNCTION".to_string()),
                ("database", "sales".to_string()),
                ("function", "calculate_tax".to_string()),
                ("pageToken", " \t".to_string()),
            ]
        );
        assert_eq!(
            ListPermissionsRequest::new(PermissionResource::view("sales", "daily_orders"))
                .query_params()
                .unwrap(),
            vec![
                ("resourceType", "VIEW".to_string()),
                ("database", "sales".to_string()),
                ("view", "daily_orders".to_string()),
            ]
        );
        let mut request = ListPermissionsRequest::new(PermissionResource::database("sales"));
        request.access = Some("SELECT".to_string());
        assert!(request
            .query_params()
            .unwrap_err()
            .to_string()
            .contains("not valid for DATABASE"));
        let mut request = ListPermissionsRequest::new(PermissionResource::catalog());
        request.principal = Some("p".repeat(129));
        assert!(request
            .query_params()
            .unwrap_err()
            .to_string()
            .contains("128"));
        for max_results in [0, 1001] {
            let mut request = ListPermissionsRequest::new(PermissionResource::catalog());
            request.max_results = Some(max_results);
            assert!(
                request
                    .query_params()
                    .unwrap_err()
                    .to_string()
                    .contains("1000"),
                "{max_results}"
            );
        }
    }

    /// Java's `String.trim` strips only characters `<= U+0020`, so an em space or a
    /// non-breaking space is a legal principal, not a blank one.
    #[test]
    fn test_is_blank_matches_java_trim_semantics() {
        for blank in ["", " ", "\t", "\n", " \r\n\t "] {
            assert!(is_blank(blank), "{blank:?}");
        }
        for present in ["\u{2003}", "\u{00a0}", "a", " a "] {
            assert!(!is_blank(present), "{present:?}");
        }
        assert!(PermissionAssignment::validate_principal("\u{2003}").is_ok());
        let mut request = ListPermissionsRequest::new(PermissionResource::catalog());
        request.principal = Some("\u{2003}".to_string());
        assert_eq!(
            request.query_params().unwrap(),
            vec![
                ("resourceType", "CATALOG".to_string()),
                ("principal", "\u{2003}".to_string()),
            ]
        );
    }

    const PREDICATE_JSON: &str = r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"region","type":"STRING"}},"function":"EQUAL","literals":["APAC"]}"#;
    const TRANSFORM_JSON: &str =
        r#"{"name":"CONCAT","inputs":[{"index":0,"name":"region","type":"STRING"},"****"]}"#;

    #[test]
    fn test_policy_type_wire_names() {
        assert_eq!(
            serde_json::to_string(&PolicyType::RowFilter).unwrap(),
            r#""ROW_FILTER""#
        );
        assert_eq!(
            serde_json::from_str::<PolicyType>(r#""column_masking""#).unwrap(),
            PolicyType::ColumnMasking
        );
        assert!("MASK".parse::<PolicyType>().is_err());
        assert_eq!(PolicyType::ColumnMasking.to_string(), "COLUMN_MASKING");
    }

    #[test]
    fn test_policies_round_trip_java_wire_json() {
        let mask = DataPolicy::new_column_mask(
            table_resource(),
            ColumnMask::new("email", TRANSFORM_JSON).unwrap(),
            "analyst",
        )
        .unwrap();
        let json = serde_json::to_string(&mask).unwrap();
        assert_eq!(
            json,
            format!(
                r#"{{"resource":{{"type":"TABLE","database":"sales","table":"orders"}},"columnMask":{{"onColumn":"email","transform":{}}},"principal":"analyst"}}"#,
                serde_json::to_string(TRANSFORM_JSON).unwrap()
            )
        );
        let round_trip: DataPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(round_trip, mask);
        assert_eq!(round_trip.policy_type(), PolicyType::ColumnMasking);
        assert_eq!(round_trip.column_mask().unwrap().on_column(), "email");
        assert_eq!(
            round_trip.column_mask().unwrap().transform(),
            TRANSFORM_JSON
        );
        assert!(round_trip.row_filter().is_none());

        let filter = DataPolicy::new_row_filter(
            table_resource(),
            RowFilter::new(PREDICATE_JSON).unwrap(),
            "analyst",
        )
        .unwrap();
        let round_trip: DataPolicy =
            serde_json::from_str(&serde_json::to_string(&filter).unwrap()).unwrap();
        assert_eq!(round_trip.policy_type(), PolicyType::RowFilter);
        assert_eq!(round_trip.row_filter().unwrap().predicate(), PREDICATE_JSON);
        assert!(round_trip.column_mask().is_none());
        assert_eq!(round_trip.principal(), "analyst");
        assert_eq!(round_trip.resource(), &table_resource());
    }

    #[test]
    fn test_policy_validation_and_payload_bounds() {
        let message = |error: Error| error.to_string();
        assert!(message(RowFilter::new(" ").unwrap_err()).contains("predicate cannot be empty"));
        assert!(message(ColumnMask::new("email", " ").unwrap_err())
            .contains("transform cannot be empty"));
        assert!(message(ColumnMask::new(" ", TRANSFORM_JSON).unwrap_err())
            .contains("onColumn cannot be empty"));
        assert!(message(
            DataPolicy::new_column_mask(
                PermissionResource::catalog(),
                ColumnMask::new("email", TRANSFORM_JSON).unwrap(),
                "analyst"
            )
            .unwrap_err()
        )
        .contains("only to TABLE"));
        assert!(message(
            DataPolicy::new_row_filter(
                table_resource(),
                RowFilter::new(PREDICATE_JSON).unwrap(),
                " "
            )
            .unwrap_err()
        )
        .contains("principal cannot be empty"));
        assert!(message(
            RowFilter::new("p".repeat(RowFilter::MAX_PREDICATE_BYTES + 1)).unwrap_err()
        )
        .contains("UTF-8 bytes"));
        assert!(RowFilter::new("p".repeat(RowFilter::MAX_PREDICATE_BYTES)).is_ok());
        assert!(message(
            ColumnMask::new("email", "t".repeat(ColumnMask::MAX_TRANSFORM_BYTES + 1)).unwrap_err()
        )
        .contains("UTF-8 bytes"));
        assert!(PermissionResource::column("sales", "orders")
            .validate_policy_attachment()
            .is_err());
        assert!(table_resource().validate_policy_attachment().is_ok());
    }

    #[test]
    fn test_policy_deserialization_runs_the_same_checks_as_the_constructors() {
        let rejected = |json: &str| {
            serde_json::from_str::<DataPolicy>(json)
                .unwrap_err()
                .to_string()
        };
        let policy = |definition: &str| -> String {
            rejected(&format!(
                r#"{{"resource":{{"type":"TABLE","database":"sales","table":"orders"}},{definition}"principal":"analyst"}}"#
            ))
        };
        assert!(serde_json::from_str::<RowFilter>(r#"{"predicate":"   "}"#)
            .unwrap_err()
            .to_string()
            .contains("predicate cannot be empty"));
        assert!(serde_json::from_value::<RowFilter>(
            serde_json::json!({"predicate": "p".repeat(RowFilter::MAX_PREDICATE_BYTES + 1)})
        )
        .unwrap_err()
        .to_string()
        .contains("UTF-8 bytes"));
        assert!(
            serde_json::from_str::<ColumnMask>(r#"{"onColumn":"  ","transform":"  "}"#)
                .unwrap_err()
                .to_string()
                .contains("onColumn cannot be empty")
        );
        assert!(policy(r#""rowFilter":{"predicate":"  "},"#).contains("predicate cannot be empty"));
        assert!(
            policy(r#""columnMask":{"onColumn":"email","transform":" "},"#)
                .contains("transform cannot be empty")
        );
        for definition in [
            "",
            r#""rowFilter":{"predicate":"{}"},"columnMask":{"onColumn":"email","transform":"{}"},"#,
        ] {
            assert!(policy(definition).contains("exactly one"));
        }
        assert!(rejected(
            r#"{"resource":{"type":"TABLE","database":"sales","table":"orders"},"rowFilter":{"predicate":"{}"},"principal":" "}"#
        )
        .contains("principal cannot be empty"));
        assert!(rejected(
            r#"{"resource":{"type":"TABLE","database":"sales","table":""},"rowFilter":{"predicate":"{}"},"principal":"analyst"}"#
        )
        .contains("table is required for TABLE"));

        // A blank locator that canonicalizing can drop is corrected, not rejected, on the way in.
        let corrected: DataPolicy = serde_json::from_str(
            r#"{"resource":{"type":"TABLE","database":"sales","table":"orders","view":""},"rowFilter":{"predicate":"{}"},"principal":"analyst"}"#,
        )
        .unwrap();
        assert_eq!(corrected.resource(), &table_resource());
    }

    #[test]
    fn test_list_policies_request_query_params() {
        let mut request = ListPoliciesRequest::new(table_resource());
        request.policy_type = Some(PolicyType::ColumnMasking);
        request.principal = Some("analyst".to_string());
        request.column = Some("email".to_string());
        request.max_results = Some(25);
        request.page_token = Some(" \t".to_string());
        assert_eq!(
            request.query_params().unwrap(),
            vec![
                ("type", "COLUMN_MASKING".to_string()),
                ("principal", "analyst".to_string()),
                ("column", "email".to_string()),
                ("maxResults", "25".to_string()),
                ("pageToken", " \t".to_string()),
            ]
        );
        assert!(ListPoliciesRequest::new(table_resource())
            .query_params()
            .unwrap()
            .is_empty());
        let mut request = ListPoliciesRequest::new(table_resource());
        request.column = Some("email".to_string());
        assert!(request
            .query_params()
            .unwrap_err()
            .to_string()
            .contains("COLUMN_MASKING"));
        let mut request = ListPoliciesRequest::new(table_resource());
        request.max_results = Some(1001);
        assert!(request
            .query_params()
            .unwrap_err()
            .to_string()
            .contains("1000"));
        assert!(ListPoliciesRequest::new(PermissionResource::catalog())
            .query_params()
            .unwrap_err()
            .to_string()
            .contains("only to TABLE"));
    }
}
