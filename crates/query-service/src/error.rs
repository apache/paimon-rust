// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, LookupError>;

#[derive(Debug)]
pub enum LookupError {
    Paimon(paimon::Error),
    PaimonUnavailable,
    LoadCancelled,
    InvalidRequest(String),
    InvalidPolicy(String),
    UnsupportedKeyType {
        field: String,
        data_type: String,
    },
    InvalidKeyValue {
        field: String,
        message: String,
    },
    QueryBudgetExceeded {
        files: usize,
        bytes: u64,
        max_files: usize,
        max_bytes: u64,
    },
    SnapshotMismatch {
        expected: i64,
        actual: Option<i64>,
    },
    InvalidDescriptor {
        field: String,
        message: String,
    },
    UnexpectedResult(String),
    Shared(Arc<LookupError>),
}

impl Display for LookupError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paimon(error) => Display::fmt(error, f),
            Self::PaimonUnavailable => write!(f, "Paimon metadata or data is unavailable"),
            Self::LoadCancelled => write!(f, "in-flight descriptor lookup was cancelled"),
            Self::InvalidRequest(message) => write!(f, "invalid lookup request: {message}"),
            Self::InvalidPolicy(message) => write!(f, "invalid lookup policy: {message}"),
            Self::UnsupportedKeyType { field, data_type } => {
                write!(f, "unsupported lookup key type for '{field}': {data_type}")
            }
            Self::InvalidKeyValue { field, message } => {
                write!(f, "invalid value for lookup key '{field}': {message}")
            }
            Self::QueryBudgetExceeded {
                files,
                bytes,
                max_files,
                max_bytes,
            } => write!(
                f,
                "lookup plan exceeds budget: files={files}/{max_files}, bytes={bytes}/{max_bytes}"
            ),
            Self::SnapshotMismatch { expected, actual } => write!(
                f,
                "lookup planned an unexpected snapshot: expected={expected}, actual={actual:?}"
            ),
            Self::InvalidDescriptor { field, message } => {
                write!(f, "invalid BlobDescriptor in field '{field}': {message}")
            }
            Self::UnexpectedResult(message) => write!(f, "unexpected lookup result: {message}"),
            Self::Shared(error) => Display::fmt(error, f),
        }
    }
}

impl std::error::Error for LookupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Paimon(error) => Some(error),
            Self::Shared(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<paimon::Error> for LookupError {
    fn from(value: paimon::Error) -> Self {
        Self::Paimon(value)
    }
}
