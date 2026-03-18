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

//! REST API error types.
//!
//! This module provides error types for REST API operations,
//! mapping HTTP status codes to specific error variants.

use reqwest::StatusCode;
use serde_json::Value;
use snafu::Snafu;

/// REST API error type.
#[derive(Debug, Snafu)]
pub enum RestError {
    /// Bad request error (HTTP 400)
    #[snafu(display("Bad request: {}", message))]
    BadRequest { message: String },

    /// Not authorized error (HTTP 401)
    #[snafu(display("Not authorized: {}", message))]
    NotAuthorized { message: String },

    /// Forbidden error (HTTP 403)
    #[snafu(display("Forbidden: {}", message))]
    Forbidden { message: String },

    /// Resource not found error (HTTP 404)
    #[snafu(display("Resource not found: {:?} named {:?}: {}", resource_type, resource_name, message))]
    NoSuchResource {
        resource_type: Option<String>,
        resource_name: Option<String>,
        message: String,
    },

    /// Resource already exists error (HTTP 409)
    #[snafu(display("Resource already exists: {:?} named {:?}: {}", resource_type, resource_name, message))]
    AlreadyExists {
        resource_type: Option<String>,
        resource_name: Option<String>,
        message: String,
    },

    /// Service failure error (HTTP 500)
    #[snafu(display("Service failure: {}", message))]
    ServiceFailure { message: String },

    /// Not implemented error (HTTP 501)
    #[snafu(display("Not implemented: {}", message))]
    NotImplemented { message: String },

    /// Service unavailable error (HTTP 503)
    #[snafu(display("Service unavailable: {}", message))]
    ServiceUnavailable { message: String },

    /// Unexpected error
    #[snafu(display("Unexpected error: {}", message))]
    Unexpected { message: String },
}

/// Parsed error information from HTTP response
pub struct ErrorInfo {
    pub message: Option<String>,
    pub resource_type: Option<String>,
    pub resource_name: Option<String>,
}

impl RestError {
    /// Parse error response body to extract error details
    pub fn parse_error_response(text: &str) -> ErrorInfo {
        let maybe_json: serde_json::Result<Value> = serde_json::from_str(text);

        if let Ok(json) = maybe_json {
            ErrorInfo {
                message: json
                    .get("message")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string()),
                resource_type: json
                    .get("resourceType")
                    .or_else(|| json.get("resource_type"))
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string()),
                resource_name: json
                    .get("resourceName")
                    .or_else(|| json.get("resource_name"))
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string()),
            }
        } else {
            ErrorInfo {
                message: None,
                resource_type: None,
                resource_name: None,
            }
        }
    }

    /// Map HTTP status code to corresponding error type
    pub fn from_status(status: StatusCode, message: String, error_info: ErrorInfo) -> Self {
        match status {
            StatusCode::BAD_REQUEST => RestError::BadRequest { message },
            StatusCode::UNAUTHORIZED => RestError::NotAuthorized { message },
            StatusCode::FORBIDDEN => RestError::Forbidden { message },
            StatusCode::NOT_FOUND => RestError::NoSuchResource {
                resource_type: error_info.resource_type,
                resource_name: error_info.resource_name,
                message,
            },
            StatusCode::CONFLICT => RestError::AlreadyExists {
                resource_type: error_info.resource_type,
                resource_name: error_info.resource_name,
                message,
            },
            StatusCode::INTERNAL_SERVER_ERROR => RestError::ServiceFailure { message },
            StatusCode::NOT_IMPLEMENTED => RestError::NotImplemented { message },
            StatusCode::SERVICE_UNAVAILABLE => RestError::ServiceUnavailable { message },
            _ => RestError::Unexpected { message },
        }
    }
}
