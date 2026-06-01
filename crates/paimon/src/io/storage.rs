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
#[cfg(any(
    feature = "storage-azdls",
    feature = "storage-cos",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-obs",
    feature = "storage-s3",
    feature = "storage-hdfs"
))]
use std::sync::Mutex;
#[cfg(any(
    feature = "storage-azdls",
    feature = "storage-cos",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-obs",
    feature = "storage-s3"
))]
use std::sync::MutexGuard;

#[cfg(feature = "storage-azdls")]
use super::AzdlsStorageConfig;
#[cfg(feature = "storage-cos")]
use opendal::services::CosConfig;
#[cfg(feature = "storage-gcs")]
use opendal::services::GcsConfig;
#[cfg(feature = "storage-hdfs")]
use opendal::services::HdfsNativeConfig;
#[cfg(feature = "storage-obs")]
use opendal::services::ObsConfig;
#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
use opendal::{Operator, Scheme};
#[cfg(any(
    feature = "storage-cos",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-obs",
    feature = "storage-s3"
))]
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
    #[cfg(feature = "storage-cos")]
    Cos {
        config: Box<CosConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-azdls")]
    Azdls {
        config: Box<AzdlsStorageConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-obs")]
    Obs {
        config: Box<ObsConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs {
        config: Box<GcsConfig>,
        operators: Mutex<HashMap<String, Operator>>,
    },
    #[cfg(feature = "storage-hdfs")]
    Hdfs {
        config: Box<HdfsNativeConfig>,
        op: Mutex<Option<Operator>>,
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
            #[cfg(feature = "storage-cos")]
            Scheme::Cos => {
                let config = super::cos_config_parse(props)?;
                Ok(Self::Cos {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let config = super::azdls_config_parse(props)?;
                Ok(Self::Azdls {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-obs")]
            Scheme::Obs => {
                let config = super::obs_config_parse(props)?;
                Ok(Self::Obs {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => {
                let config = super::gcs_config_parse(props)?;
                Ok(Self::Gcs {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-hdfs")]
            Scheme::HdfsNative => {
                let config = super::hdfs_config_parse(props)?;
                Ok(Self::Hdfs {
                    config: Box::new(config),
                    op: Mutex::new(None),
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
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "OSS", &["oss"])?;
                let op = Self::cached_oss_operator(config, operators, path, &bucket)?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "S3", &["s3", "s3a"])?;
                let op = Self::cached_s3_operator(config, operators, path, &bucket)?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-cos")]
            Storage::Cos { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "COS", &["cos", "cosn"])?;
                let op = Self::cached_operator(operators, "COS", &bucket, || {
                    super::cos_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-azdls")]
            Storage::Azdls { config, operators } => {
                let relative_path = super::azdls_relative_path(path)?;
                let cache_key = super::azdls_operator_cache_key(config, path)?;
                let op = Self::cached_operator(operators, "Azure", &cache_key, || {
                    super::azdls_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-obs")]
            Storage::Obs { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "OBS", &["obs"])?;
                let op = Self::cached_operator(operators, "OBS", &bucket, || {
                    super::obs_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-gcs")]
            Storage::Gcs { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "GCS", &["gcs", "gs"])?;
                let op = Self::cached_operator(operators, "GCS", &bucket, || {
                    super::gcs_config_build(config, path)
                })?;
                Ok((op, relative_path))
            }
            #[cfg(feature = "storage-hdfs")]
            Storage::Hdfs { config, op } => {
                let relative_path = super::hdfs_relative_path(path)?;
                let mut guard = op.lock().map_err(|_| error::Error::UnexpectedError {
                    message: "Failed to lock HDFS operator".to_string(),
                    source: None,
                })?;
                // HDFS uses a single operator per Storage instance (unlike S3/OSS
                // which cache per bucket). The operator is lazily initialized from
                // the first path's NameNode if not set in config. One FileIO
                // instance should target exactly one HDFS cluster.
                if guard.is_none() {
                    *guard = Some(super::hdfs_config_build(config, path)?);
                }
                Ok((guard.as_ref().unwrap().clone(), relative_path))
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

    #[cfg(any(
        feature = "storage-cos",
        feature = "storage-gcs",
        feature = "storage-obs",
        feature = "storage-oss",
        feature = "storage-s3"
    ))]
    fn bucket_and_relative_path<'a>(
        path: &'a str,
        storage_name: &str,
        allowed_schemes: &[&str],
    ) -> crate::Result<(String, &'a str)> {
        let url = Url::parse(path).map_err(|_| error::Error::ConfigInvalid {
            message: format!("Invalid {storage_name} url: {path}"),
        })?;
        let bucket = url
            .host_str()
            .ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid {storage_name} url: {path}, missing bucket"),
            })?
            .to_string();
        let scheme = url.scheme();
        if !allowed_schemes.contains(&scheme) {
            return Err(error::Error::ConfigInvalid {
                message: format!("Invalid {storage_name} url: {path}, unsupported scheme {scheme}"),
            });
        }
        let prefix = format!("{scheme}://{bucket}/");
        let relative_path =
            path.strip_prefix(&prefix)
                .ok_or_else(|| error::Error::ConfigInvalid {
                    message: format!(
                        "Invalid {storage_name} url: {path}, should start with {prefix}"
                    ),
                })?;
        Ok((bucket, relative_path))
    }

    #[cfg(any(
        feature = "storage-azdls",
        feature = "storage-cos",
        feature = "storage-gcs",
        feature = "storage-oss",
        feature = "storage-obs",
        feature = "storage-s3"
    ))]
    fn lock_operator_cache<'a>(
        operators: &'a Mutex<HashMap<String, Operator>>,
        storage_name: &str,
    ) -> crate::Result<MutexGuard<'a, HashMap<String, Operator>>> {
        operators.lock().map_err(|_| error::Error::UnexpectedError {
            message: format!("Failed to lock {storage_name} operator cache"),
            source: None,
        })
    }

    #[cfg(any(
        feature = "storage-azdls",
        feature = "storage-cos",
        feature = "storage-gcs",
        feature = "storage-oss",
        feature = "storage-obs",
        feature = "storage-s3"
    ))]
    fn cached_operator(
        operators: &Mutex<HashMap<String, Operator>>,
        storage_name: &str,
        cache_key: &str,
        build: impl FnOnce() -> crate::Result<Operator>,
    ) -> crate::Result<Operator> {
        let mut operators = Self::lock_operator_cache(operators, storage_name)?;
        if let Some(op) = operators.get(cache_key) {
            return Ok(op.clone());
        }

        let op = build()?;
        operators.insert(cache_key.to_string(), op.clone());
        Ok(op)
    }

    #[cfg(feature = "storage-oss")]
    fn cached_oss_operator(
        config: &OssConfig,
        operators: &Mutex<HashMap<String, Operator>>,
        path: &str,
        bucket: &str,
    ) -> crate::Result<Operator> {
        Self::cached_operator(operators, "OSS", bucket, || {
            super::oss_config_build(config, path)
        })
    }

    #[cfg(feature = "storage-s3")]
    fn cached_s3_operator(
        config: &S3Config,
        operators: &Mutex<HashMap<String, Operator>>,
        path: &str,
        bucket: &str,
    ) -> crate::Result<Operator> {
        Self::cached_operator(operators, "S3", bucket, || {
            super::s3_config_build(config, path)
        })
    }

    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "cosn" => Ok(Scheme::Cos),
            "abfs" | "abfss" | "az" | "azure" => Ok(Scheme::Azdls),
            "gs" => Ok(Scheme::Gcs),
            "hdfs" => Ok(Scheme::HdfsNative),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}
