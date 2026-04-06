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

use std::collections::HashMap;
#[cfg(any(feature = "storage-oss", feature = "storage-s3"))]
use std::sync::{Mutex, MutexGuard};

#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
use opendal::{Operator, Scheme};
#[cfg(any(feature = "storage-oss", feature = "storage-s3"))]
use url::Url;

use crate::error;

use super::FileIOBuilder;

/// The storage carries all supported storage services in paimon
#[derive(Debug)]
pub enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory { op: Operator },
    #[cfg(feature = "storage-fs")]
    LocalFs { op: Operator },
    #[cfg(feature = "storage-oss")]
    Oss {
        config: Box<OssConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-s3")]
    S3 {
        config: Box<S3Config>,
        operators: Mutex<HashMap<String, Operator>>,
    },
}

impl Storage {
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, props) = file_io_builder.into_parts();
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory {
                op: super::memory_config_build()?,
            }),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs {
                op: super::fs_config_build()?,
            }),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => {
                let config = super::oss_config_parse(props)?;
                Ok(Self::Oss {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => {
                let config = super::s3_config_parse(props)?;
                Ok(Self::S3 {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            _ => Err(error::Error::IoUnsupported {
                message: "Unsupported storage feature".to_string(),
            }),
        }
    }

    pub(crate) fn create<'a>(&self, path: &'a str) -> crate::Result<(Operator, &'a str)> {
        match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory { op } => Ok((op.clone(), Self::memory_relative_path(path)?)),
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs { op } => Ok((op.clone(), Self::fs_relative_path(path)?)),
            #[cfg(feature = "storage-oss")]
            Storage::Oss { config, operators } => {
                let (bucket, relative_path) = Self::oss_bucket_and_relative_path(path)?;
                let op = Self::cached_oss_operator(config, operators, path, &bucket)?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 { config, operators } => {
                let (bucket, relative_path) = Self::s3_bucket_and_relative_path(path)?;
                let op = Self::cached_s3_operator(config, operators, path, &bucket)?;
                Ok((op, relative_path))
            }
        }
    }

    #[cfg(feature = "storage-memory")]
    fn memory_relative_path(path: &str) -> crate::Result<&str> {
        if let Some(stripped) = path.strip_prefix("memory:/") {
            Ok(stripped)
        } else {
            path.get(1..).ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid memory path: {path}"),
            })
        }
    }

    #[cfg(feature = "storage-fs")]
    fn fs_relative_path(path: &str) -> crate::Result<&str> {
        if let Some(stripped) = path.strip_prefix("file:/") {
            Ok(stripped)
        } else {
            path.get(1..).ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid file path: {path}"),
            })
        }
    }

    #[cfg(feature = "storage-oss")]
    fn oss_bucket_and_relative_path(path: &str) -> crate::Result<(String, &str)> {
        let url = Url::parse(path).map_err(|_| error::Error::ConfigInvalid {
            message: format!("Invalid OSS url: {path}"),
        })?;
        let bucket = url
            .host_str()
            .ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid OSS url: {path}, missing bucket"),
            })?
            .to_string();
        let prefix = format!("oss://{bucket}/");
        let relative_path =
            path.strip_prefix(&prefix)
                .ok_or_else(|| error::Error::ConfigInvalid {
                    message: format!("Invalid OSS url: {path}, should start with {prefix}"),
                })?;
        Ok((bucket, relative_path))
    }

    #[cfg(feature = "storage-s3")]
    fn s3_bucket_and_relative_path(path: &str) -> crate::Result<(String, &str)> {
        let url = Url::parse(path).map_err(|_| error::Error::ConfigInvalid {
            message: format!("Invalid S3 url: {path}"),
        })?;
        let bucket = url
            .host_str()
            .ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid S3 url: {path}, missing bucket"),
            })?
            .to_string();
        let scheme = url.scheme();
        let prefix = match scheme {
            "s3" | "s3a" => format!("{scheme}://{bucket}/"),
            _ => {
                return Err(error::Error::ConfigInvalid {
                    message: format!(
                        "Invalid S3 url: {path}, should start with s3://{bucket}/ or s3a://{bucket}/"
                    ),
                });
            }
        };
        let relative_path =
            path.strip_prefix(&prefix)
                .ok_or_else(|| error::Error::ConfigInvalid {
                    message: format!(
                    "Invalid S3 url: {path}, should start with s3://{bucket}/ or s3a://{bucket}/"
                ),
                })?;
        Ok((bucket, relative_path))
    }

    #[cfg(any(feature = "storage-oss", feature = "storage-s3"))]
    fn lock_operator_cache<'a>(
        operators: &'a Mutex<HashMap<String, Operator>>,
        storage_name: &str,
    ) -> crate::Result<MutexGuard<'a, HashMap<String, Operator>>> {
        operators.lock().map_err(|_| error::Error::UnexpectedError {
            message: format!("Failed to lock {storage_name} operator cache"),
            source: None,
        })
    }

    #[cfg(feature = "storage-oss")]
    fn cached_oss_operator(
        config: &OssConfig,
        operators: &Mutex<HashMap<String, Operator>>,
        path: &str,
        bucket: &str,
    ) -> crate::Result<Operator> {
        let mut operators = Self::lock_operator_cache(operators, "OSS")?;
        if let Some(op) = operators.get(bucket) {
            return Ok(op.clone());
        }

        let op = super::oss_config_build(config, path)?;
        operators.insert(bucket.to_string(), op.clone());
        Ok(op)
    }

    #[cfg(feature = "storage-s3")]
    fn cached_s3_operator(
        config: &S3Config,
        operators: &Mutex<HashMap<String, Operator>>,
        path: &str,
        bucket: &str,
    ) -> crate::Result<Operator> {
        let mut operators = Self::lock_operator_cache(operators, "S3")?;
        if let Some(op) = operators.get(bucket) {
            return Ok(op.clone());
        }

        let op = super::s3_config_build(config, path)?;
        operators.insert(bucket.to_string(), op.clone());
        Ok(op)
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
