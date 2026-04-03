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

use snafu::prelude::*;

/// Result type used in paimon.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for paimon.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(whatever, display("Paimon data invalid for {}: {:?}", message, source))]
    DataInvalid {
        message: String,
        /// see https://github.com/shepmaster/snafu/issues/446
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync + 'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unsupported error {}", message)
    )]
    Unsupported { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected error {}: {:?}", message, source)
    )]
    UnexpectedError {
        message: String,
        #[snafu(source(false))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon data type invalid for {}", message)
    )]
    DataTypeInvalid { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected error {}: {:?}", message, source)
    )]
    IoUnexpected {
        message: String,
        #[snafu(source(from(opendal::Error, Box::new)))]
        source: Box<opendal::Error>,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unsupported io error {}", message)
    )]
    IoUnsupported { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting invalid config: {}", message)
    )]
    ConfigInvalid { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting invalid file index format: {}", message)
    )]
    FileIndexFormatInvalid { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected parquet error: {}", message)
    )]
    ParquetDataUnexpected {
        message: String,
        source: Box<parquet::errors::ParquetError>,
    },

    // ======================= catalog errors ===============================
    #[snafu(display("Database {} already exists.", database))]
    DatabaseAlreadyExist { database: String },
    #[snafu(display("Database {} does not exist.", database))]
    DatabaseNotExist { database: String },
    #[snafu(display("Database {} is not empty.", database))]
    DatabaseNotEmpty { database: String },
    #[snafu(display("Table {} already exists.", full_name))]
    TableAlreadyExist { full_name: String },
    #[snafu(display("Table {} does not exist.", full_name))]
    TableNotExist { full_name: String },
    #[snafu(display("Column {} already exists in table {}.", column, full_name))]
    ColumnAlreadyExist { full_name: String, column: String },
    #[snafu(display("Column {} does not exist in table {}.", column, full_name))]
    ColumnNotExist { full_name: String, column: String },
    #[snafu(display("Invalid identifier: {}", message))]
    IdentifierInvalid { message: String },

    // ======================= REST API errors ===============================
    #[snafu(display("{}", source))]
    RestApi {
        #[snafu(source)]
        source: crate::api::rest_error::RestError,
    },
}

impl From<opendal::Error> for Error {
    fn from(source: opendal::Error) -> Self {
        // TODO: Simple use IoUnexpected for now
        Error::IoUnexpected {
            message: "IO operation failed on underlying storage".to_string(),
            source: Box::new(source),
        }
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(source: parquet::errors::ParquetError) -> Self {
        Error::ParquetDataUnexpected {
            message: format!("Failed to read a Parquet file: {source}"),
            source: Box::new(source),
        }
    }
}

impl From<crate::api::rest_error::RestError> for Error {
    fn from(source: crate::api::rest_error::RestError) -> Self {
        Error::RestApi { source }
    }
}
