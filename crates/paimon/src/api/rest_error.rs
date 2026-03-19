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
    #[snafu(display(
        "Resource not found: {:?} named {:?}: {}",
        resource_type,
        resource_name,
        message
    ))]
    NoSuchResource {
        resource_type: Option<String>,
        resource_name: Option<String>,
        message: String,
    },

    /// Resource already exists error (HTTP 409)
    #[snafu(display(
        "Resource already exists: {:?} named {:?}: {}",
        resource_type,
        resource_name,
        message
    ))]
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

use super::api_response::ErrorResponse;

impl RestError {
    /// Parse error response body to ErrorResponse
    ///
    /// If the response body cannot be parsed as JSON, or if the message is missing,
    /// creates an ErrorResponse with the raw response body as the message and the
    /// HTTP status code as the code.
    pub fn parse_error_response(text: &str, status_code: u16) -> ErrorResponse {
        if let Ok(error) = serde_json::from_str::<ErrorResponse>(text) {
            if error.message.is_some() {
                return error;
            }
            // If message is missing, create a new ErrorResponse with the raw text
            return ErrorResponse::new(
                error.resource_type,
                error.resource_name,
                Some(text.to_string()),
                error.code.or(Some(status_code as i32)),
            );
        }

        // Failed to parse JSON, create ErrorResponse from raw text
        ErrorResponse::new(
            None,
            None,
            Some(if text.is_empty() {
                "response body is null".to_string()
            } else {
                text.to_string()
            }),
            Some(status_code as i32),
        )
    }

    /// Map ErrorResponse code to corresponding error type
    pub fn from_error_response(error: ErrorResponse) -> Self {
        let code = error.code.unwrap_or(500);
        let message = error
            .message
            .clone()
            .unwrap_or_else(|| "Unknown error".to_string());

        match code {
            400 => RestError::BadRequest { message },
            401 => RestError::NotAuthorized { message },
            403 => RestError::Forbidden { message },
            404 => RestError::NoSuchResource {
                resource_type: error.resource_type,
                resource_name: error.resource_name,
                message,
            },
            409 => RestError::AlreadyExists {
                resource_type: error.resource_type,
                resource_name: error.resource_name,
                message,
            },
            500 => RestError::ServiceFailure { message },
            501 => RestError::NotImplemented { message },
            503 => RestError::ServiceUnavailable { message },
            _ => RestError::Unexpected { message },
        }
    }
}
