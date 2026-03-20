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

//! REST API module for Paimon.
//!
//! This module provides REST API client, request, and response types.

pub mod api_request;
pub mod auth;
pub mod resource_paths;
pub mod rest_api;
pub mod rest_client;
pub mod rest_error;
pub mod rest_util;

mod api_response;

// Re-export request types
pub use api_request::{
    AlterDatabaseRequest, CreateDatabaseRequest, CreateTableRequest, RenameTableRequest,
};

// Re-export response types
pub use api_response::{
    AuditRESTResponse, ConfigResponse, ErrorResponse, GetDatabaseResponse, GetTableResponse,
    ListDatabasesResponse, ListTablesResponse, PagedList,
};

// Re-export error types
pub use rest_error::RestError;

// Re-export utility types
pub use resource_paths::ResourcePaths;
pub use rest_util::RESTUtil;

// Re-export auth types
pub use auth::{
    AuthProvider, AuthProviderFactory, BearerTokenAuthProvider, RESTAuthFunction, RESTAuthParameter,
};
