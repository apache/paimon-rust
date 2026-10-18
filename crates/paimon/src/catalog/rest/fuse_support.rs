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

//! Resolve and validate the REST catalog's local FUSE mount.

use crate::api::rest_api::RESTApi;
use crate::catalog::Identifier;
use crate::common::{FuseOptions, Options};
use crate::io::cache::LocalCache;
use crate::io::{FileIO, FileIOCacheContext};
use crate::{Error, Result};
use std::sync::Arc;
use tokio::sync::OnceCell;

/// One validation result per catalog, shared by tables and dependency reads.
#[derive(Debug)]
pub(crate) struct FusePathResolver {
    options: Options,
    validation: OnceCell<bool>,
}

impl FusePathResolver {
    pub(crate) fn new(options: &Options) -> Self {
        Self {
            options: options.clone(),
            validation: OnceCell::new(),
        }
    }

    pub(crate) async fn file_io(
        &self,
        original: &str,
        identifier: &Identifier,
        api: &RESTApi,
        local_cache: Option<Arc<LocalCache>>,
        file_io_cache: FileIOCacheContext,
    ) -> Result<Option<FileIO>> {
        if !self
            .options
            .get(FuseOptions::ENABLED)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
        {
            return Ok(None);
        }
        let local = self.resolve(original, identifier.database(), Some(identifier.object()))?;
        let validation_mode = self
            .options
            .get(FuseOptions::VALIDATION_MODE)
            .map(String::as_str)
            .unwrap_or("strict");
        if !matches!(validation_mode, "strict" | "warn" | "none") {
            return Err(Error::ConfigInvalid { message: format!("Invalid fuse.validation-mode: {validation_mode}; expected strict, warn or none") });
        }
        let valid = self
            .validation
            .get_or_try_init(|| self.validate(api, validation_mode))
            .await?;
        if !valid {
            return Ok(None);
        }
        #[cfg(feature = "storage-fs")]
        {
            let local = Self::local_path(&local)?;
            let local = local.to_string_lossy();
            let mut builder = FileIO::from_path(local.as_ref())?
                .with_props(self.options.to_map())
                .with_cache_context(file_io_cache);
            if let Some(cache) = local_cache {
                builder = builder.with_local_cache(cache);
            }
            Ok(Some(builder.build()?.with_fuse(original, &local)?))
        }
        #[cfg(not(feature = "storage-fs"))]
        {
            let _ = (local, local_cache, file_io_cache);
            Err(Error::Unsupported {
                message: "FUSE requires the storage-fs feature".into(),
            })
        }
    }

    fn resolve(&self, original: &str, database: &str, table: Option<&str>) -> Result<String> {
        let root = self
            .options
            .get(FuseOptions::ROOT)
            .filter(|root| !root.is_empty())
            .ok_or_else(|| Error::ConfigInvalid {
                message: "FUSE is enabled but fuse.root is not configured".into(),
            })?;
        let root = root.trim_end_matches('/');
        match self
            .options
            .get(FuseOptions::MODE)
            .map(String::as_str)
            .unwrap_or("pvfs")
        {
            "pvfs" => Ok(match table {
                Some(table) => format!("{root}/{database}/{table}"),
                None => format!("{root}/{database}"),
            }),
            "raw" => {
                // Keep literal spaces, percent escapes and Unicode unchanged,
                // matching urllib.parse rather than URL-normalizing object keys.
                let original = original.split(['?', '#']).next().unwrap_or_default();
                let scheme = original.split_once(':').filter(|(scheme, _)| {
                    scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                        && scheme
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
                });
                let mut path = scheme.map_or(original, |(_, rest)| rest);
                if let Some(authority) = path.strip_prefix("//") {
                    path = authority.find('/').map_or("", |index| &authority[index..]);
                }
                let mut path = path.trim_start_matches('/');
                if scheme.is_none() {
                    if let Some((_, suffix)) = path.split_once('/') {
                        path = suffix;
                    }
                }
                Ok(format!("{root}/{path}"))
            }
            mode => Err(Error::ConfigInvalid {
                message: format!("Invalid fuse.mode: {mode}; expected pvfs or raw"),
            }),
        }
    }

    fn local_path(value: &str) -> Result<std::path::PathBuf> {
        let path = if value
            .get(..5)
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("file:"))
        {
            let uri = url::Url::parse(value).map_err(|error| Error::ConfigInvalid {
                message: format!("Invalid FUSE file URI '{value}': {error}"),
            })?;
            #[cfg(not(windows))]
            if let Some(host) = uri.host_str().filter(|host| *host != "localhost") {
                let path = percent_encoding::percent_decode_str(uri.path()).decode_utf8_lossy();
                return std::path::absolute(format!("//{host}{path}")).map_err(|error| {
                    Error::UnexpectedError {
                        message: "Cannot resolve the FUSE mount directory".into(),
                        source: Some(Box::new(error)),
                    }
                });
            }
            uri.to_file_path().map_err(|_| Error::ConfigInvalid {
                message: format!("Invalid FUSE file URI '{value}'"),
            })?
        } else {
            std::path::PathBuf::from(value)
        };
        std::path::absolute(path).map_err(|error| Error::UnexpectedError {
            message: "Cannot resolve the FUSE mount directory".into(),
            source: Some(Box::new(error)),
        })
    }

    async fn validate(&self, api: &RESTApi, mode: &str) -> Result<bool> {
        if mode == "none" {
            return Ok(true);
        }
        // Even warn mode propagates REST errors; only a missing mount falls back.
        let database = api.get_database("default").await?;
        let Some(location) = database.location.filter(|path| !path.is_empty()) else {
            return Ok(true);
        };
        let local = self.resolve(&location, "default", None)?;
        let path = Self::local_path(&local)?;
        match tokio::fs::metadata(&path).await {
            Ok(_) => Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                let message = format!(
                    "FUSE local path validation failed: '{local}' does not exist for '{location}'"
                );
                if mode == "warn" {
                    log::warn!("{message}. Falling back to default FileIO.");
                    Ok(false)
                } else {
                    Err(Error::ConfigInvalid { message })
                }
            }
            Err(error) => Err(Error::UnexpectedError {
                message: format!("Cannot validate FUSE mount {local}"),
                source: Some(Box::new(error)),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuse_layouts_preserve_identifiers_and_literal_uri_paths() {
        let mut options = Options::new();
        options.set(FuseOptions::ROOT, "/mnt/fuse/");
        let resolver = FusePathResolver::new(&options);
        assert_eq!(
            resolver
                .resolve("oss://bucket/physical", "db 空格", Some("t$branch_dev"))
                .unwrap(),
            "/mnt/fuse/db 空格/t$branch_dev"
        );
        assert_eq!(
            resolver
                .resolve("oss://bucket/physical", "default", None)
                .unwrap(),
            "/mnt/fuse/default"
        );
        options.set(FuseOptions::MODE, "raw");
        let resolver = FusePathResolver::new(&options);
        for (original, expected) in [
            (
                "oss://bucket/a%2Fb/空 格?t=1#fragment",
                "/mnt/fuse/a%2Fb/空 格",
            ),
            ("memory:/warehouse/db/t", "/mnt/fuse/warehouse/db/t"),
            ("catalog/db/t", "/mnt/fuse/db/t"),
            ("/catalog/db/t", "/mnt/fuse/db/t"),
            ("table", "/mnt/fuse/table"),
        ] {
            assert_eq!(
                resolver
                    .resolve(original, "ignored", Some("ignored"))
                    .unwrap(),
                expected
            );
        }
        options.set(FuseOptions::ROOT, "/");
        assert_eq!(
            FusePathResolver::new(&options)
                .resolve("oss://bucket/db/t", "db", Some("t"))
                .unwrap(),
            "/db/t"
        );
    }

    #[test]
    fn file_uri_mounts_decode_paths_but_plain_paths_keep_percent_escapes() {
        let root = tempfile::tempdir().unwrap();
        let mount = root.path().join("fuse mount%");
        let uri = url::Url::from_file_path(&mount).unwrap().to_string();
        assert_eq!(FusePathResolver::local_path(&uri).unwrap(), mount);
        let plain = root.path().join("fuse%20mount");
        assert_eq!(
            FusePathResolver::local_path(plain.to_str().unwrap()).unwrap(),
            plain
        );
    }

    #[test]
    fn fuse_layout_requires_a_root_and_known_mode() {
        let mut options = Options::new();
        assert!(FusePathResolver::new(&options)
            .resolve("oss://bucket/t", "db", Some("t"))
            .is_err());
        options.set(FuseOptions::ROOT, "/mnt");
        options.set(FuseOptions::MODE, "invalid");
        assert!(FusePathResolver::new(&options)
            .resolve("oss://bucket/t", "db", Some("t"))
            .unwrap_err()
            .to_string()
            .contains("fuse.mode"));
    }
}
