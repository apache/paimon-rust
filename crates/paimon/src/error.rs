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

pub(crate) const JINDO_FORK_ERROR: &str =
    "Jindo SDK cannot be reused after process fork; use spawn or avoid initializing Jindo in the parent process";

/// Result type used in paimon.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for paimon.
#[derive(Debug, Snafu)]
pub enum Error {
    /// A configured resource budget could not admit the requested reservation.
    #[snafu(display("Paimon resource exhausted: {}", message))]
    ResourceExhausted { message: String },
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
    #[snafu(display("{}", message))]
    ProcessForkUnsupported { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting invalid config: {}", message)
    )]
    ConfigInvalid { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected avro error {}: {:?}", message, source)
    )]
    DataUnexpected {
        message: String,
        source: Box<apache_avro::Error>,
    },
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
    #[snafu(display("Snapshot {} does not exist.", snapshot_id))]
    SnapshotNotExist { snapshot_id: i64 },
    #[snafu(display("Tag {} already exists.", tag_name))]
    TagAlreadyExist { tag_name: String },
    #[snafu(display("Tag {} does not exist.", tag_name))]
    TagNotExist { tag_name: String },
    #[snafu(display("View {} already exists.", full_name))]
    ViewAlreadyExist { full_name: String },
    #[snafu(display("View {} does not exist.", full_name))]
    ViewNotExist { full_name: String },
    #[snafu(display("Function {} does not exist.", full_name))]
    FunctionNotExist { full_name: String },
    #[snafu(display("Function {} already exists.", full_name))]
    FunctionAlreadyExist { full_name: String },
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
        Error::from_opendal_with_context(source, "IO operation failed on underlying storage")
    }
}

impl Error {
    pub(crate) fn from_opendal_with_context(
        source: opendal::Error,
        message: impl Into<String>,
    ) -> Self {
        if source.kind() == opendal::ErrorKind::Unsupported && source.message() == JINDO_FORK_ERROR
        {
            return Error::ProcessForkUnsupported {
                message: source.message().to_string(),
            };
        }
        Error::IoUnexpected {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Whether this error or one of its wrapped causes rejects inherited native state.
    #[doc(hidden)]
    pub fn is_process_fork_unsupported(&self) -> bool {
        match self {
            Error::ProcessForkUnsupported { .. } => true,
            Error::DataInvalid { source, .. } | Error::UnexpectedError { source, .. } => source
                .as_deref()
                .is_some_and(|source| error_chain_contains_process_fork(source)),
            Error::IoUnexpected { source, .. } => is_jindo_fork_error(source),
            Error::DataUnexpected { source, .. } => error_chain_contains_process_fork(source),
            Error::ParquetDataUnexpected { source, .. } => {
                error_chain_contains_process_fork(source)
            }
            Error::RestApi { source } => error_chain_contains_process_fork(source),
            _ => false,
        }
    }
}

fn is_jindo_fork_error(error: &opendal::Error) -> bool {
    error.kind() == opendal::ErrorKind::Unsupported && error.message() == JINDO_FORK_ERROR
}

fn error_chain_contains_process_fork(error: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(error) = error.downcast_ref::<Error>() {
        return error.is_process_fork_unsupported();
    }
    if let Some(error) = error.downcast_ref::<opendal::Error>() {
        return is_jindo_fork_error(error);
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        if error
            .get_ref()
            .is_some_and(|source| error_chain_contains_process_fork(source))
        {
            return true;
        }
    }
    error
        .source()
        .is_some_and(error_chain_contains_process_fork)
}

impl From<apache_avro::Error> for Error {
    fn from(source: apache_avro::Error) -> Self {
        Error::DataUnexpected {
            message: "Failed to process Avro data".to_string(),
            source: Box::new(source),
        }
    }
}

impl Error {
    pub(crate) fn from_parquet_with_context(
        source: parquet::errors::ParquetError,
        context: &str,
    ) -> Self {
        let source = match source {
            parquet::errors::ParquetError::External(source) => match source.downcast::<Error>() {
                Ok(source) => return *source,
                Err(source) => parquet::errors::ParquetError::External(source),
            },
            source => source,
        };
        Error::ParquetDataUnexpected {
            message: format!("{context}: {source}"),
            source: Box::new(source),
        }
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(source: parquet::errors::ParquetError) -> Self {
        Error::from_parquet_with_context(source, "Failed to read a Parquet file")
    }
}

impl From<crate::api::rest_error::RestError> for Error {
    fn from(source: crate::api::rest_error::RestError) -> Self {
        Error::RestApi { source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_jindo_fork_error_kind() {
        let error: Error =
            opendal::Error::new(opendal::ErrorKind::Unsupported, JINDO_FORK_ERROR).into();
        assert!(matches!(error, Error::ProcessForkUnsupported { .. }));
    }

    #[test]
    fn preserves_paimon_error_through_parquet_external_error() {
        let parquet_error =
            parquet::errors::ParquetError::External(Box::new(Error::ProcessForkUnsupported {
                message: JINDO_FORK_ERROR.to_string(),
            }));

        let error: Error = parquet_error.into();
        assert!(matches!(error, Error::ProcessForkUnsupported { .. }));
    }

    #[test]
    fn keeps_non_paimon_parquet_external_error_wrapped() {
        let parquet_error = parquet::errors::ParquetError::External(Box::new(
            std::io::Error::other("external parquet failure"),
        ));

        let error: Error = parquet_error.into();
        assert!(matches!(error, Error::ParquetDataUnexpected { .. }));
    }

    #[test]
    fn detects_wrapped_process_fork_error() {
        let error = Error::UnexpectedError {
            message: "outer context".to_string(),
            source: Some(Box::new(Error::IoUnexpected {
                message: "storage context".to_string(),
                source: Box::new(opendal::Error::new(
                    opendal::ErrorKind::Unsupported,
                    JINDO_FORK_ERROR,
                )),
            })),
        };

        assert!(error.is_process_fork_unsupported());
    }
}
