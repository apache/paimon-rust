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

use std::borrow::Cow;
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
#[cfg(feature = "storage-oss")]
use super::OssStorageConfig;
use opendal::Operator;
#[cfg(feature = "storage-cos")]
use opendal_service_cos::CosConfig;
#[cfg(feature = "storage-gcs")]
use opendal_service_gcs::GcsConfig;
#[cfg(feature = "storage-hdfs")]
use opendal_service_hdfs_native::HdfsNativeConfig;
#[cfg(feature = "storage-obs")]
use opendal_service_obs::ObsConfig;
#[cfg(feature = "storage-s3")]
use opendal_service_s3::S3Config;
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
    /// A caller-provided opendal operator, explicitly scoped to filesystem semantics
    /// (see `FileIOBuilder::with_fs_operator`).
    CustomFs { op: Operator },
    #[cfg(feature = "storage-memory")]
    Memory { op: Operator },
    #[cfg(feature = "storage-fs")]
    LocalFs { op: Operator },
    #[cfg(feature = "storage-oss")]
    Oss {
        config: Box<OssStorageConfig>,
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
        let (scheme_str, props, operator) = file_io_builder.into_parts();
        if let Some(op) = operator {
            return Ok(Self::CustomFs { op });
        }
        let scheme = scheme_str.to_ascii_lowercase();
        match scheme.as_str() {
            #[cfg(feature = "storage-memory")]
            "memory" => Ok(Self::Memory {
                op: super::memory_config_build()?,
            }),
            #[cfg(feature = "storage-fs")]
            "file" | "fs" | "" => Ok(Self::LocalFs {
                op: super::fs_config_build()?,
            }),
            #[cfg(feature = "storage-oss")]
            "oss" => {
                let config = super::oss_config_parse(props)?;
                Ok(Self::Oss {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-s3")]
            "s3" | "s3a" => {
                let config = super::s3_config_parse(props)?;
                Ok(Self::S3 {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-cos")]
            "cos" | "cosn" => {
                let config = super::cos_config_parse(props)?;
                Ok(Self::Cos {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-azdls")]
            "abfs" | "abfss" | "az" | "azdfs" | "azdls" | "azure" => {
                let config = super::azdls_config_parse(props)?;
                Ok(Self::Azdls {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-obs")]
            "obs" => {
                let config = super::obs_config_parse(props)?;
                Ok(Self::Obs {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-gcs")]
            "gcs" | "gs" => {
                let config = super::gcs_config_parse(props)?;
                Ok(Self::Gcs {
                    config: Box::new(config),
                    operators: Mutex::new(HashMap::new()),
                })
            }
            #[cfg(feature = "storage-hdfs")]
            "hdfs" | "hdfs-native" | "hdfs_native" => {
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

    pub(crate) fn create<'a>(&self, path: &'a str) -> crate::Result<(Operator, Cow<'a, str>)> {
        match self {
            Storage::CustomFs { op } => {
                // The custom operator is a *filesystem* operator: paths resolve with the
                // local-filesystem rules below. A scheme'd path would be silently mangled by
                // those rules (`s3://bucket/a` → `3://bucket/a`), so reject it instead —
                // object-store operators need the bucket/scheme resolution the built-in
                // backends perform, which this hook deliberately does not reimplement.
                if !path.starts_with("file:/") && path.contains("://") {
                    return Err(error::Error::ConfigInvalid {
                        message: format!(
                            "the custom operator is a filesystem operator and cannot resolve \
                             the scheme'd path: {path}"
                        ),
                    });
                }
                Ok((op.clone(), Self::fs_relative_path(path)?))
            }
            #[cfg(feature = "storage-memory")]
            Storage::Memory { op } => {
                Ok((op.clone(), Cow::Borrowed(Self::memory_relative_path(path)?)))
            }
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs { op } => Ok((op.clone(), Self::fs_relative_path(path)?)),
            #[cfg(feature = "storage-oss")]
            Storage::Oss { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "OSS", &["oss"])?;
                let op = Self::cached_oss_operator(config, operators, path, &bucket)?;
                Ok((op, Cow::Borrowed(relative_path)))
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "S3", &["s3", "s3a"])?;
                let op = Self::cached_s3_operator(config, operators, path, &bucket)?;
                Ok((op, Cow::Borrowed(relative_path)))
            }
            #[cfg(feature = "storage-cos")]
            Storage::Cos { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "COS", &["cos", "cosn"])?;
                let op = Self::cached_operator(operators, "COS", &bucket, || {
                    super::cos_config_build(config, path)
                })?;
                Ok((op, Cow::Borrowed(relative_path)))
            }
            #[cfg(feature = "storage-azdls")]
            Storage::Azdls { config, operators } => {
                let relative_path = super::azdls_relative_path(path)?;
                let cache_key = super::azdls_operator_cache_key(config, path)?;
                let op = Self::cached_operator(operators, "Azure", &cache_key, || {
                    super::azdls_config_build(config, path)
                })?;
                Ok((op, Cow::Borrowed(relative_path)))
            }
            #[cfg(feature = "storage-obs")]
            Storage::Obs { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "OBS", &["obs"])?;
                let op = Self::cached_operator(operators, "OBS", &bucket, || {
                    super::obs_config_build(config, path)
                })?;
                Ok((op, Cow::Borrowed(relative_path)))
            }
            #[cfg(feature = "storage-gcs")]
            Storage::Gcs { config, operators } => {
                let (bucket, relative_path) =
                    Self::bucket_and_relative_path(path, "GCS", &["gcs", "gs"])?;
                let op = Self::cached_operator(operators, "GCS", &bucket, || {
                    super::gcs_config_build(config, path)
                })?;
                Ok((op, Cow::Borrowed(relative_path)))
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
                Ok((
                    guard.as_ref().unwrap().clone(),
                    Cow::Borrowed(relative_path),
                ))
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

    /// Turn an absolute local path into the relative path that opendal's `fs`
    /// service joins onto its `/` root.
    ///
    /// On POSIX an absolute path `/tmp/wh` becomes `tmp/wh`: dropping the single
    /// leading separator lets opendal rebuild `/tmp/wh` from its `/` root.
    ///
    /// A bare drop-the-first-char would corrupt a Windows path such as
    /// `C:\dir` into `:\dir` (the drive letter is lost — the historical source
    /// of the "invalid filename" failures on Windows). Instead we keep the
    /// drive specifier and only normalize separators to `/`, mirroring how Java
    /// Paimon's `Path` (modeled on Hadoop's) handles Windows paths. opendal then
    /// does `PathBuf::from("/").join("C:/dir")`, and because the argument
    /// carries a drive prefix `Path::join` replaces the base, yielding the real
    /// `C:\dir` on Windows.
    fn fs_relative_path(path: &str) -> crate::Result<Cow<'_, str>> {
        // A `file://` / `file:/` URL is already in scheme-relative form.
        if let Some(stripped) = path.strip_prefix("file:/") {
            return Ok(if cfg!(windows) && stripped.contains('\\') {
                Cow::Owned(stripped.replace('\\', "/"))
            } else {
                Cow::Borrowed(stripped)
            });
        }
        if super::looks_like_windows_drive_path(path) {
            return Ok(Cow::Owned(path.replace('\\', "/")));
        }
        path.get(1..)
            .map(Cow::Borrowed)
            .ok_or_else(|| error::Error::ConfigInvalid {
                message: format!("Invalid file path: {path}"),
            })
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
        config: &OssStorageConfig,
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
}

#[cfg(test)]
mod scheme_tests {
    use crate::error::Error;
    use crate::io::{FileIOBuilder, Storage};

    fn build(scheme: &str) -> Storage {
        Storage::build(FileIOBuilder::new(scheme)).unwrap()
    }

    #[cfg(feature = "storage-memory")]
    #[test]
    fn memory_scheme_is_case_insensitive() {
        for scheme in ["memory", "MEMORY"] {
            assert!(matches!(build(scheme), Storage::Memory { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-fs")]
    #[test]
    fn local_fs_scheme_aliases_are_compatible() {
        for scheme in ["", "file", "FILE", "fs", "FS"] {
            assert!(matches!(build(scheme), Storage::LocalFs { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-oss")]
    #[test]
    fn oss_scheme_is_case_insensitive() {
        for scheme in ["oss", "OSS"] {
            let storage = Storage::build(FileIOBuilder::new(scheme).with_props([
                ("fs.oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com"),
                ("fs.oss.accessKeyId", "test-ak"),
                ("fs.oss.accessKeySecret", "test-sk"),
            ]))
            .unwrap();
            assert!(matches!(storage, Storage::Oss { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-s3")]
    #[test]
    fn s3_scheme_aliases_are_compatible() {
        for scheme in ["s3", "S3", "s3a", "S3A"] {
            assert!(matches!(build(scheme), Storage::S3 { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-cos")]
    #[test]
    fn cos_scheme_aliases_are_compatible() {
        for scheme in ["cos", "COS", "cosn", "COSN"] {
            assert!(matches!(build(scheme), Storage::Cos { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-azdls")]
    #[test]
    fn azdls_scheme_aliases_are_compatible() {
        for scheme in [
            "azdls", "AZDLS", "azdfs", "AZDFS", "abfs", "ABFS", "abfss", "ABFSS", "az", "AZ",
            "azure", "AZURE",
        ] {
            assert!(matches!(build(scheme), Storage::Azdls { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-obs")]
    #[test]
    fn obs_scheme_is_case_insensitive() {
        for scheme in ["obs", "OBS"] {
            assert!(matches!(build(scheme), Storage::Obs { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-gcs")]
    #[test]
    fn gcs_scheme_aliases_are_compatible() {
        for scheme in ["gcs", "GCS", "gs", "GS"] {
            assert!(matches!(build(scheme), Storage::Gcs { .. }), "{scheme}");
        }
    }

    #[cfg(feature = "storage-hdfs")]
    #[test]
    fn hdfs_native_scheme_aliases_are_compatible() {
        for scheme in [
            "hdfs",
            "HDFS",
            "hdfs-native",
            "HDFS-NATIVE",
            "hdfs_native",
            "HDFS_NATIVE",
        ] {
            assert!(matches!(build(scheme), Storage::Hdfs { .. }), "{scheme}");
        }
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        let error = FileIOBuilder::new("unknown").build().unwrap_err();
        assert!(matches!(error, Error::IoUnsupported { .. }));
    }
}

#[cfg(all(test, feature = "storage-fs"))]
mod fs_relative_path_tests {
    use super::Storage;

    fn rel(path: &str) -> String {
        Storage::fs_relative_path(path).unwrap().into_owned()
    }

    #[test]
    fn posix_absolute_path_drops_leading_separator() {
        // opendal joins the result onto its `/` root, rebuilding `/tmp/wh`.
        assert_eq!(rel("/tmp/wh"), "tmp/wh");
        assert_eq!(rel("/tmp/wh/db.db/t"), "tmp/wh/db.db/t");
    }

    #[test]
    fn file_scheme_is_stripped() {
        assert_eq!(rel("file:/tmp/wh"), "tmp/wh");
        // `file://` keeps the leading authority slash, matching prior behavior.
        assert_eq!(rel("file:///tmp/wh"), "//tmp/wh");
    }

    #[cfg(not(windows))]
    #[test]
    fn file_scheme_preserves_posix_backslashes() {
        assert_eq!(
            rel(r"file:///tmp/consumer/consumer-id\part"),
            r"//tmp/consumer/consumer-id\part"
        );
    }

    #[test]
    fn windows_drive_path_keeps_drive_and_normalizes_separators() {
        // The historical bug dropped the drive letter (`C:\wh` -> `:\wh`); we
        // must keep it and only switch `\` to `/` so opendal's
        // `PathBuf::from("/").join(..)` rebuilds the real `C:\wh` on Windows.
        assert_eq!(rel(r"C:\Users\wh"), "C:/Users/wh");
        assert_eq!(rel("C:/Users/wh"), "C:/Users/wh");
        assert_eq!(rel(r"D:\a\b\c"), "D:/a/b/c");
    }

    #[test]
    fn windows_mixed_separators_are_normalized() {
        // make_path concatenates with `/`, so a Windows warehouse yields a
        // mixed-separator path that must still normalize cleanly.
        assert_eq!(rel(r"C:\Users\wh/db.db/t"), "C:/Users/wh/db.db/t");
    }
}

#[cfg(all(test, feature = "storage-memory"))]
mod tests {
    use super::*;

    fn memory_operator() -> Operator {
        Operator::from_config(opendal::services::MemoryConfig::default()).unwrap()
    }

    /// The filesystem operator resolves absolute paths and `file:` URLs with the
    /// local-filesystem rules, against the operator's own root.
    #[test]
    fn custom_fs_operator_resolves_filesystem_paths() {
        let storage = Storage::CustomFs {
            op: memory_operator(),
        };
        let (_, relative) = storage.create("/warehouse/db/table").unwrap();
        assert_eq!(relative, "warehouse/db/table");
        let (_, relative) = storage.create("file:/warehouse/db/table").unwrap();
        assert_eq!(relative, "warehouse/db/table");
    }

    /// A scheme'd path would be silently mangled by the filesystem rules
    /// (`s3://bucket/a` → `3://bucket/a`), so it is rejected instead.
    #[test]
    fn custom_fs_operator_rejects_schemed_paths() {
        let storage = Storage::CustomFs {
            op: memory_operator(),
        };
        for path in ["s3://bucket/a", "oss://bucket/a", "hdfs://nn/a"] {
            assert!(
                storage.create(path).is_err(),
                "expected {path} to be rejected by the filesystem operator"
            );
        }
    }
}
