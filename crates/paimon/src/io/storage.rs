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

use opendal::{Operator, Scheme};

use crate::error;

use super::FileIOBuilder;

/// The storage carries all supported storage services in paimon
#[derive(Debug)]
pub enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory,
    #[cfg(feature = "storage-fs")]
    LocalFs,
}

impl Storage {
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, _) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            _ => Err(error::Error::IoUnsupported {
                message: "Unsupported storage feature".to_string(),
            }),
        }
    }

    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> crate::Result<(Operator, &'a str)> {
        let path = path.as_ref();
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory => {
                let op = super::memory_config_build()?;

                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs => {
                let op = super::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok((op, stripped))
                } else {
                    Ok((op, &path[1..]))
                }
            }
        }
    }

    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}
