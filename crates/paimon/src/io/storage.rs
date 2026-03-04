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

#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
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
    #[cfg(feature = "storage-oss")]
    Oss { config: Box<OssConfig> },
    #[cfg(feature = "storage-s3")]
    S3 { config: Box<S3Config> },
}

impl Storage {
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, _props) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => {
                let config = super::oss_config_parse(_props)?;
                Ok(Self::Oss {
                    config: Box::new(config),
                })
            }
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => {
                let config = super::s3_config_parse(_props)?;
                Ok(Self::S3 {
                    config: Box::new(config),
                })
            }
            _ => Err(error::Error::IoUnsupported {
                message: "Unsupported storage feature".to_string(),
            }),
        }
    }

    pub(crate) fn build_operator(&self) -> crate::Result<Operator> {
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory => super::memory_config_build(),
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs => super::fs_config_build(),
        }
    }

    pub(crate) fn relative_path<'a>(&self, path: &'a str) -> crate::Result<&'a str> {
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok(stripped)
                } else {
                    path.get(1..).ok_or_else(|| error::Error::ConfigInvalid {
                        message: format!("Invalid memory path: {path}"),
                    })
                }
            }
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs => {
                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok(stripped)
                } else {
                    path.get(1..).ok_or_else(|| error::Error::ConfigInvalid {
                        message: format!("Invalid file path: {path}"),
                    })
                }
            }
            #[cfg(feature = "storage-oss")]
            Storage::Oss { config } => {
                let op = super::oss_config_build(config, path)?;
                let prefix = format!("oss://{}/", op.info().name());
                if let Some(stripped) = path.strip_prefix(&prefix) {
                    Ok((op, stripped))
                } else {
                    Err(error::Error::ConfigInvalid {
                        message: format!("Invalid OSS url: {path}, should start with {prefix}"),
                    })
                }
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 { config } => {
                let op = super::s3_config_build(config, path)?;
                // Support both s3:// and s3a:// URL prefixes.
                let info = op.info();
                let bucket = info.name();
                let s3_prefix = format!("s3://{}/", bucket);
                let s3a_prefix = format!("s3a://{}/", bucket);
                if let Some(stripped) = path.strip_prefix(&s3_prefix) {
                    Ok((op, stripped))
                } else if let Some(stripped) = path.strip_prefix(&s3a_prefix) {
                    Ok((op, stripped))
                } else {
                    Err(error::Error::ConfigInvalid {
                        message: format!(
                            "Invalid S3 url: {path}, should start with {s3_prefix} or {s3a_prefix}"
                        ),
                    })
                }
            }
        }
    }

    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}
